package com.github.lisama569.quorumreplicationlogjava.replication;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * 进程内模拟集群：持有节点与网络分区状态，实现 {@link Transport}。
 *
 * <ul>
 *   <li>分区：通过节点对断连集合模拟，消息直接丢弃（返回空）；</li>
 *   <li>重启：丢弃节点易失状态，复用其 {@link DurableStore}（模拟磁盘保留）；</li>
 *   <li>路由：消息按目标节点 ID 直接方法调用投递。</li>
 * </ul>
 */
public class Cluster implements Transport {

    private final List<String> nodeIds;
    private final Map<String, RaftNode> nodes = new LinkedHashMap<>();
    /** 每个节点的“磁盘”，重启后保留。 */
    private final Map<String, DurableStore> disks = new HashMap<>();
    /** 断连的节点对（无向，存双向）。 */
    private final Set<Set<String>> linksDown = new HashSet<>();
    /** 响应方注入的处理延迟（毫秒），用于模拟确认迟迟不回应。 */
    private final Map<String, Long> latencyMillis = new HashMap<>();
    /** 一致性读的确认时限（毫秒）。 */
    private volatile long readConfirmTimeoutMillis = 1000;
    /** 发送方节点 ID（由发送线程在调用前绑定）。 */
    private final ThreadLocal<String> sender = new ThreadLocal<>();

    public Cluster(List<String> nodeIds) {
        this.nodeIds = List.copyOf(nodeIds);
        for (String id : nodeIds) {
            disks.put(id, new InMemoryDurableStore());
            nodes.put(id, new RaftNode(id, nodeIds.size(), disks.get(id)));
        }
    }

    public RaftNode node(String nodeId) {
        return nodes.get(nodeId);
    }

    public Map<String, RaftNode> nodes() {
        return Collections.unmodifiableMap(nodes);
    }

    public List<String> nodeIds() {
        return nodeIds;
    }

    /** 除自身外的其他节点 ID。 */
    public List<String> peersOf(String nodeId) {
        List<String> peers = new ArrayList<>(nodeIds);
        peers.remove(nodeId);
        return peers;
    }

    /** 以指定节点身份发起一次写入（绑定发送方，用于分区判定）。 */
    public WriteResult write(String nodeId, String requestId, String payload) {
        sender.set(nodeId);
        try {
            RaftNode node = nodes.get(nodeId);
            return node.clientWrite(this, peersOf(nodeId), requestId, payload);
        } finally {
            sender.remove();
        }
    }

    /** 以指定节点身份发起选举。 */
    public boolean elect(String nodeId) {
        sender.set(nodeId);
        try {
            RaftNode node = nodes.get(nodeId);
            return node.startElection(this, peersOf(nodeId));
        } finally {
            sender.remove();
        }
    }

    /** 以指定领导者身份触发一轮日志复制/心跳。 */
    public void replicate(String nodeId) {
        sender.set(nodeId);
        try {
            RaftNode node = nodes.get(nodeId);
            node.replicate(this, peersOf(nodeId));
        } finally {
            sender.remove();
        }
    }

    /** 以指定节点身份发起一次一致性读（绑定发送方，用于分区判定）。 */
    public ReadResult read(String nodeId) {
        sender.set(nodeId);
        try {
            RaftNode node = nodes.get(nodeId);
            return node.clientRead(this, peersOf(nodeId), readConfirmTimeoutMillis);
        } finally {
            sender.remove();
        }
    }

    /** 设置一致性读的确认时限（毫秒）。 */
    public void setReadConfirmTimeoutMillis(long millis) {
        this.readConfirmTimeoutMillis = millis;
    }

    /** 给某节点的响应注入处理延迟（毫秒），模拟确认迟迟不回应。 */
    public synchronized void setLatency(String nodeId, long millis) {
        latencyMillis.put(nodeId, millis);
    }

    /** 清除某节点的注入延迟。 */
    public synchronized void clearLatency(String nodeId) {
        latencyMillis.remove(nodeId);
    }

    private void applyLatency(String targetId) {
        long delay;
        synchronized (this) {
            delay = latencyMillis.getOrDefault(targetId, 0L);
        }
        if (delay > 0) {
            try {
                Thread.sleep(delay);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /** 断开两个节点间的通信（双向）。 */
    public synchronized void disconnect(String a, String b) {
        linksDown.add(Set.of(a, b));
    }

    /** 恢复两个节点间的通信（双向）。 */
    public synchronized void connect(String a, String b) {
        linksDown.remove(Set.of(a, b));
    }

    /** 将节点与其余所有节点隔离。 */
    public void isolate(String nodeId) {
        for (String other : nodeIds) {
            if (!other.equals(nodeId)) {
                disconnect(nodeId, other);
            }
        }
    }

    /** 恢复节点与所有节点的通信。 */
    public void heal(String nodeId) {
        for (String other : nodeIds) {
            if (!other.equals(nodeId)) {
                connect(nodeId, other);
            }
        }
    }

    /** 模拟节点崩溃重启：丢弃易失状态，保留磁盘（DurableStore）。 */
    public void restart(String nodeId) {
        nodes.put(nodeId, new RaftNode(nodeId, nodeIds.size(), disks.get(nodeId)));
    }

    private boolean reachable(String from, String to) {
        synchronized (this) {
            return !linksDown.contains(Set.of(from, to));
        }
    }

    @Override
    public Optional<VoteResponse> sendVoteRequest(String targetId, VoteRequest request) {
        String from = sender.get();
        if (from == null || !reachable(from, targetId)) {
            return Optional.empty();
        }
        applyLatency(targetId);
        return Optional.of(nodes.get(targetId).handleVoteRequest(request));
    }

    @Override
    public Optional<AppendResponse> sendAppendRequest(String targetId, AppendRequest request) {
        String from = sender.get();
        if (from == null || !reachable(from, targetId)) {
            return Optional.empty();
        }
        applyLatency(targetId);
        return Optional.of(nodes.get(targetId).handleAppendRequest(request));
    }

    @Override
    public Optional<ReadConfirmResponse> sendReadConfirm(String targetId, ReadConfirmRequest request) {
        String from = sender.get();
        if (from == null || !reachable(from, targetId)) {
            return Optional.empty();
        }
        applyLatency(targetId);
        return Optional.of(nodes.get(targetId).handleReadConfirm(request));
    }
}
