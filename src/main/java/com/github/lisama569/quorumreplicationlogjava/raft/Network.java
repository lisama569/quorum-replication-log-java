package com.github.lisama569.quorumreplicationlogjava.raft;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 进程内模拟网络：RPC 同步投递（调用线程直接进入目标节点的加锁处理函数），
 * 可注入对称分区并恢复；节点停机时投递以 {@link RejectReason#NODE_DOWN} 失败，
 * 被分区时以 {@link RejectReason#NETWORK_PARTITION} 失败，二者可区分。
 */
public class Network {

    private static final Logger log = LoggerFactory.getLogger(Network.class);

    private final Map<String, RaftNode> nodes = new HashMap<>();
    /** 不可达的无序节点对。 */
    private final Set<Long> blockedPairs = new HashSet<>();

    public synchronized void register(RaftNode node) {
        nodes.put(node.id(), node);
    }

    public synchronized RaftNode get(String id) {
        RaftNode node = nodes.get(id);
        if (node == null) {
            throw new ReplicatedLogException(RejectReason.NODE_DOWN, "未知节点: " + id);
        }
        return node;
    }

    private static long pairKey(String a, String b) {
        long h1 = Math.abs((long) a.hashCode());
        long h2 = Math.abs((long) b.hashCode());
        return Math.min(h1, h2) * 1_000_003L + Math.max(h1, h2);
    }

    public synchronized boolean isReachable(String from, String to) {
        if (from.equals(to)) {
            return true;
        }
        RaftNode target = nodes.get(to);
        if (target == null || !target.isRunning()) {
            return false;
        }
        return !blockedPairs.contains(pairKey(from, to));
    }

    public synchronized Set<String> reachablePeers(String from) {
        Set<String> result = new HashSet<>();
        for (String id : nodes.keySet()) {
            if (!id.equals(from) && isReachable(from, id)) {
                result.add(id);
            }
        }
        return Collections.unmodifiableSet(result);
    }

    /** 在两组节点之间注入双向分区（组内仍互通）。 */
    public synchronized void addPartition(Set<String> groupA, Set<String> groupB) {
        List<Long> added = new ArrayList<>();
        for (String a : groupA) {
            for (String b : groupB) {
                long key = pairKey(a, b);
                blockedPairs.add(key);
                added.add(key);
            }
        }
        log.warn("[NET] 注入分区 groupA={} groupB={} 阻断链路数={}", groupA, groupB, added.size());
    }

    /** 恢复全部网络分区。 */
    public synchronized void heal() {
        int n = blockedPairs.size();
        blockedPairs.clear();
        log.warn("[NET] 分区全部恢复，解除链路数={}", n);
    }

    private RaftNode requireReachable(String from, String to) {
        RaftNode target = get(to);
        if (!target.isRunning()) {
            throw new ReplicatedLogException(RejectReason.NODE_DOWN,
                    "节点 " + to + " 已停机，消息无法投递 from=" + from);
        }
        if (!from.equals(to) && blockedPairs.contains(pairKey(from, to))) {
            throw new ReplicatedLogException(RejectReason.NETWORK_PARTITION,
                    "网络分区：" + from + " 无法到达 " + to);
        }
        return target;
    }

    public RequestVoteResponse sendRequestVote(String from, String to, RequestVoteRequest request) {
        RaftNode target;
        synchronized (this) {
            target = requireReachable(from, to);
        }
        return target.tryHandleRequestVote(request);
    }

    public AppendEntriesResponse sendAppendEntries(String from, String to, AppendEntriesRequest request) {
        RaftNode target;
        synchronized (this) {
            target = requireReachable(from, to);
        }
        return target.tryHandleAppendEntries(request);
    }
}
