package com.github.lisama569.quorumreplicationlogjava.raft;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 进程内 Raft 集群：节点 + 模拟网络 + 每节点一个守护调度线程（按固定 tick 周期驱动）。
 *
 * <p>重启语义：{@link #stopNode(String)} 只停止调度、丢弃易失状态（角色/状态机），
 * 持久化文件保留；{@link #restartNode(String)} 重新创建节点与空状态机，从磁盘恢复
 * 任期、投票与日志，再靠领导者心跳补齐落后部分。
 */
public class RaftCluster implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(RaftCluster.class);

    private final int nodeCount;
    private final Path dataDir;
    private final long tickMillis;
    private final Network network = new Network();
    private final Map<String, RaftNode> nodes = new LinkedHashMap<>();
    private final Map<String, ScheduledExecutorService> schedulers = new LinkedHashMap<>();
    private final List<String> ids = new ArrayList<>();

    public RaftCluster(int nodeCount, Path dataDir) {
        this(nodeCount, dataDir, 10);
    }

    public RaftCluster(int nodeCount, Path dataDir, long tickMillis) {
        if (nodeCount < 1 || nodeCount % 2 == 0) {
            throw new IllegalArgumentException("节点数必须为正奇数，当前=" + nodeCount);
        }
        this.nodeCount = nodeCount;
        this.dataDir = dataDir;
        this.tickMillis = tickMillis;
        for (int i = 1; i <= nodeCount; i++) {
            ids.add("n" + i);
        }
        for (String id : ids) {
            createAndRegisterNode(id);
        }
    }

    private void createAndRegisterNode(String id) {
        List<String> peers = ids.stream().filter(other -> !other.equals(id)).toList();
        FilePersistentState ps = new FilePersistentState(dataDir.toString(), id);
        KVStateMachine sm = new KVStateMachine();
        RaftNode node = new RaftNode(id, peers, network, ps, sm);
        // tick=10ms 时给 500 个 tick 的确认等待（约 5s），避免慢机误报超时
        node.setSubmitTimeoutTicks(500);
        network.register(node);
        nodes.put(id, node);
    }

    public Network network() {
        return network;
    }

    public RaftNode node(String id) {
        RaftNode node = nodes.get(id);
        if (node == null) {
            throw new ReplicatedLogException(RejectReason.NODE_DOWN, "未知节点: " + id);
        }
        return node;
    }

    public List<RaftNode> nodes() {
        return List.copyOf(nodes.values());
    }

    public void start() {
        for (String id : ids) {
            startScheduling(id);
        }
        log.info("集群启动完成 nodes={} tickMillis={}", ids, tickMillis);
    }

    private void startScheduling(String id) {
        RaftNode node = nodes.get(id);
        node.start();
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "raft-tick-" + id);
            t.setDaemon(true);
            return t;
        });
        scheduler.scheduleAtFixedRate(() -> {
            try {
                node.tick();
            } catch (Throwable t) {
                log.error("[{}] tick 异常", id, t);
            }
        }, tickMillis, tickMillis, TimeUnit.MILLISECONDS);
        schedulers.put(id, scheduler);
    }

    @Override
    public void close() {
        for (ScheduledExecutorService s : schedulers.values()) {
            s.shutdownNow();
        }
        schedulers.clear();
        for (RaftNode node : nodes.values()) {
            node.shutdown();
        }
        log.info("集群已关闭");
    }

    /** 轮询等待至多 timeoutMillis 出现单一领导者，返回其节点 id。 */
    public String awaitLeader(long timeoutMillis) {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            List<String> leaders = new ArrayList<>();
            long term = -1;
            for (RaftNode node : nodes.values()) {
                if (node.isRunning() && node.role() == Role.LEADER) {
                    leaders.add(node.id());
                    term = node.term();
                }
            }
            if (leaders.size() == 1) {
                return leaders.get(0);
            }
            if (leaders.size() > 1) {
                throw new IllegalStateException("同一时刻出现多个领导者: " + leaders);
            }
            sleep(10);
        }
        throw new ReplicatedLogException(RejectReason.NO_LEADER,
                "等待领导者超时 " + timeoutMillis + "ms");
    }

    /** 等待所有在运行节点的 commitIndex 收敛一致（落后的停机节点不参与）。 */
    public void awaitConvergence(long timeoutMillis) {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            long target = nodes.values().stream()
                    .filter(RaftNode::isRunning)
                    .mapToLong(RaftNode::lastLogIndex)
                    .max().orElse(0);
            boolean converged = nodes.values().stream()
                    .filter(RaftNode::isRunning)
                    .allMatch(n -> n.lastLogIndex() == target && n.commitIndex() == target
                            && n.stateMachine().lastApplied() == target);
            if (converged) {
                return;
            }
            sleep(10);
        }
        throw new IllegalStateException("日志收敛超时");
    }

    public void addPartition(Set<String> groupA, Set<String> groupB) {
        network.addPartition(groupA, groupB);
    }

    public void heal() {
        network.heal();
    }

    public void stopNode(String id) {
        ScheduledExecutorService s = schedulers.remove(id);
        if (s != null) {
            s.shutdownNow();
        }
        node(id).shutdown();
        log.warn("节点 {} 已模拟宕机（持久化文件保留）", id);
    }

    public void restartNode(String id) {
        if (node(id).isRunning()) {
            stopNode(id);
        }
        // 重建节点：从磁盘恢复，状态机为空，等待心跳重放已提交日志
        createAndRegisterNode(id);
        startScheduling(id);
        log.warn("节点 {} 已从持久化状态重启", id);
    }

    /** 经由当前领导者写入；无领导者时以 NO_LEADER 拒绝。 */
    public String submit(String requestId, String command) {
        RaftNode leader = currentLeader();
        return leader.submit(requestId, command);
    }

    public String readQuorum(String key) {
        return currentLeader().readQuorum(key);
    }

    private RaftNode currentLeader() {
        List<RaftNode> leaders = nodes.values().stream()
                .filter(n -> n.isRunning() && n.role() == Role.LEADER)
                .toList();
        if (leaders.isEmpty()) {
            throw new ReplicatedLogException(RejectReason.NO_LEADER,
                    "当前集群无可用领导者（可能处于选举中或多数派分区）");
        }
        if (leaders.size() > 1) {
            throw new IllegalStateException("出现多个领导者: " + leaders);
        }
        return leaders.get(0);
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
