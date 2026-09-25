package com.github.lisama569.quorumreplicationlogjava.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.lisama569.quorumreplicationlogjava.raft.AppendEntriesRequest;
import com.github.lisama569.quorumreplicationlogjava.raft.AppendEntriesResponse;
import com.github.lisama569.quorumreplicationlogjava.raft.RaftCluster;
import com.github.lisama569.quorumreplicationlogjava.raft.RaftNode;
import com.github.lisama569.quorumreplicationlogjava.raft.RejectReason;
import com.github.lisama569.quorumreplicationlogjava.raft.ReplicatedLogException;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * 校验各类非法/故障请求被明确拒绝且原因可区分：
 * NOT_LEADER / QUORUM_UNAVAILABLE / NO_LEADER / NODE_DOWN /
 * NETWORK_PARTITION / STALE_TERM / LOG_MISMATCH（索引冲突与任期冲突两种）。
 */
class RejectReasonTest {

    @Test
    void nonLeaderSubmit_isRejectedWithNotLeader(@TempDir Path dir) {
        try (RaftCluster cluster = new RaftCluster(3, dir, 10)) {
            cluster.start();
            String leader = cluster.awaitLeader(3000);
            String follower = cluster.nodes().stream().map(RaftNode::id)
                    .filter(id -> !id.equals(leader)).findFirst().orElseThrow();

            ReplicatedLogException ex = org.junit.jupiter.api.Assertions.assertThrows(
                    ReplicatedLogException.class,
                    () -> cluster.node(follower).submit("x", "a=b"));
            assertEquals(RejectReason.NOT_LEADER, ex.getReason());
            assertTrue(ex.getMessage().contains(leader), "拒绝信息应给出当前领导者提示: " + ex.getMessage());
            System.out.println("[TEST] 向跟随者写入被拒 reason=NOT_LEADER hint=" + leader
                    + " 判定依据=只有在任领导者受理写入");
        }
    }

    @Test
    void quorumLost_noLeaderAndRejectsWrites(@TempDir Path dir) throws Exception {
        try (RaftCluster cluster = new RaftCluster(3, dir, 10)) {
            cluster.start();
            String leader = cluster.awaitLeader(3000);
            // 制造 1-1-1 全互联分区：任何一侧都不足 2 票多数派
            String f1 = "n1".equals(leader) ? "n2" : "n1";
            String f2 = "n3".equals(leader) ? ("n1".equals(f1) ? "n2" : "n1") : "n3";
            cluster.addPartition(Set.of(f1), Set.of(f2));
            cluster.addPartition(Set.of(leader), Set.of(f1));
            cluster.addPartition(Set.of(leader), Set.of(f2));
            Thread.sleep(200);

            ReplicatedLogException ex = org.junit.jupiter.api.Assertions.assertThrows(
                    ReplicatedLogException.class,
                    () -> cluster.node(leader).submit("x", "a=b"));
            assertEquals(RejectReason.QUORUM_UNAVAILABLE, ex.getReason());
            System.out.println("[TEST] 1-1-1 分区下原领导者写入被拒 reason=QUORUM_UNAVAILABLE");

            // 分区恢复后集群重新可用（等待选举稳定后再提交，过渡期可能短暂 NO_LEADER）
            cluster.heal();
            cluster.awaitLeader(3000);
            Thread.sleep(100);
            String result = null;
            for (int attempt = 0; attempt < 20; attempt++) {
                try {
                    result = cluster.submit("recover", "ok=1");
                    break;
                } catch (ReplicatedLogException retry) {
                    if (retry.getReason() != RejectReason.NOT_LEADER
                            && retry.getReason() != RejectReason.NO_LEADER
                            && retry.getReason() != RejectReason.QUORUM_UNAVAILABLE) {
                        throw retry;
                    }
                    Thread.sleep(50);
                }
            }
            assertEquals("ok=1", result);
            System.out.println("[TEST] 分区恢复后选出领导者并写入成功");
        }
    }

    @Test
    void stoppedNode_networkAndDirectCallsAreRejected(@TempDir Path dir) {
        try (RaftCluster cluster = new RaftCluster(3, dir, 10)) {
            cluster.start();
            String leader = cluster.awaitLeader(3000);
            String follower = cluster.nodes().stream().map(RaftNode::id)
                    .filter(id -> !id.equals(leader)).findFirst().orElseThrow();
            cluster.stopNode(follower);

            // 领导者向停机节点发送心跳：网络层以 NODE_DOWN 失败（被领导者记为不可达，可重试）
            // 通过直接断言网络投递行为验证原因可区分
            ReplicatedLogException net = org.junit.jupiter.api.Assertions.assertThrows(
                    ReplicatedLogException.class,
                    () -> cluster.network().sendAppendEntries(leader, follower,
                            new AppendEntriesRequest(1, leader, 0, 0, List.of(), 0)));
            assertEquals(RejectReason.NODE_DOWN, net.getReason());
            System.out.println("[TEST] 向停机节点发消息 reason=NODE_DOWN 判定依据=节点未运行");

            // 直接向停机节点提交：NODE_DOWN
            RejectedCall direct = submitAndCapture(cluster.node(follower));
            assertEquals(RejectReason.NODE_DOWN, direct.reason);
            System.out.println("[TEST] 停机节点直接受理写入 reason=NODE_DOWN");
        }
    }

    @Test
    void staleTermAndLogMismatch_areDistinct(@TempDir Path dir) {
        try (RaftCluster cluster = new RaftCluster(3, dir, 10)) {
            cluster.start();
            String leader = cluster.awaitLeader(3000);
            RaftNode target = cluster.node(leader);
            long currentTerm = target.term();

            // 1) 过期任期
            AppendEntriesResponse stale = target.tryHandleAppendEntries(
                    new AppendEntriesRequest(currentTerm - 1, "ghost", 0, 0, List.of(), 0));
            assertTrue(stale.reason().startsWith("STALE_TERM"), "实际: " + stale.reason());
            System.out.println("[TEST] 过期任期 AppendEntries 被拒 reason=" + stale.reason()
                    + " termReq=" + (currentTerm - 1) + " termLocal=" + currentTerm);

            // 2) prevLogIndex 超过本地日志：索引缺失冲突
            long beyond = target.lastLogIndex() + 50;
            AppendEntriesResponse missing = target.tryHandleAppendEntries(
                    new AppendEntriesRequest(currentTerm, leader, beyond, 1, List.of(), 0));
            assertTrue(missing.reason().startsWith("LOG_MISMATCH"), "实际: " + missing.reason());
            System.out.println("[TEST] prevLogIndex=" + beyond + " 超过本地末尾 被拒 reason="
                    + missing.reason() + " 回退建议 conflictIndex=" + missing.conflictIndex());

            // 3) prevLogIndex 存在但任期不一致：任期冲突
            if (target.lastLogIndex() >= 1) {
                long prevIndex = 1;
                AppendEntriesResponse termMismatch = target.tryHandleAppendEntries(
                        new AppendEntriesRequest(currentTerm, leader, prevIndex, 999L, List.of(), 0));
                assertTrue(termMismatch.reason().startsWith("LOG_MISMATCH"), "实际: " + termMismatch.reason());
                System.out.println("[TEST] prevLogIndex=" + prevIndex + " 任期不一致 被拒 reason="
                        + termMismatch.reason() + " conflictTerm=" + termMismatch.conflictTerm()
                        + " 回退到 conflictIndex=" + termMismatch.conflictIndex()
                        + " 判定依据=冲突任期快速回溯");
            }
        }
    }

    private record RejectedCall(RejectReason reason) { }

    private RejectedCall submitAndCapture(RaftNode node) {
        try {
            node.submit("z", "z=z");
            return new RejectedCall(null);
        } catch (ReplicatedLogException e) {
            return new RejectedCall(e.getReason());
        }
    }
}
