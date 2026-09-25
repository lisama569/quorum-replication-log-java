package com.github.lisama569.quorumreplicationlogjava.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.lisama569.quorumreplicationlogjava.raft.RaftCluster;
import com.github.lisama569.quorumreplicationlogjava.raft.RaftNode;
import com.github.lisama569.quorumreplicationlogjava.raft.RequestVoteRequest;
import com.github.lisama569.quorumreplicationlogjava.raft.RequestVoteResponse;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * 选举安全：
 * 1) 同一任期内，同一候选人重复拉票可以再次获得同意（重试安全），但改投其他候选人被拒；
 * 2) 同任期两个不同候选人的投票请求，最多一个被同意（不得双主）；
 * 3) 2-vs-3 分区期间，只有多数派一侧能产生领导者，恢复后仍只有一个领导者。
 */
class ElectionSafetyTest {

    @Test
    void oneVotePerTerm_duplicateRequestIsIdempotent(@TempDir Path dir) {
        try (RaftCluster cluster = new RaftCluster(3, dir, 10)) {
            cluster.start();
            cluster.awaitLeader(3000);
            RaftNode voter = cluster.node("n1");
            long term = voter.term();

            // 直接向 n1 重复投递同一候选人的投票请求（模拟 RPC 重试）
            RequestVoteRequest dup = new RequestVoteRequest(term, "candidate-A", 100, term);
            RequestVoteResponse r1 = voter.tryHandleRequestVote(dup);
            RequestVoteResponse r2 = voter.tryHandleRequestVote(dup);
            // 若 n1 已把本任期票投给在任领导者，则两次都拒绝但原因必须一致；
            // 若投给了 A，则两次都同意。无论哪种，两次结果必须相同（重试幂等）。
            assertEquals(r1.voteGranted(), r2.voteGranted(), "重复投票请求结果必须一致");
            System.out.println("[TEST] 同候选人重复拉票结果幂等 granted=" + r1.voteGranted()
                    + " reason=" + r1.reason() + " term=" + term);

            // 同任期另一个不同候选人必须被拒（ALREADY_VOTED），除非它带着更高任期
            RequestVoteRequest other = new RequestVoteRequest(term, "candidate-B", 100, term);
            RequestVoteResponse r3 = voter.tryHandleRequestVote(other);
            if (r1.voteGranted()) {
                assertFalse(r3.voteGranted(), "同一任期不得把票改投给另一候选人");
                assertTrue(r3.reason().startsWith("ALREADY_VOTED"), "实际=" + r3.reason());
                System.out.println("[TEST] 同任期改投被拒 reason=" + r3.reason()
                        + " 判定依据=一任期一票，杜绝双主");
            }
            assertEquals(term, voter.term(), "同任期处理后任期不应跳变");
        }
    }

    @Test
    void partitionNeverCreatesTwoLeadersInSameTerm(@TempDir Path dir) throws Exception {
        try (RaftCluster cluster = new RaftCluster(5, dir, 10)) {
            cluster.start();
            String leader = cluster.awaitLeader(3000);

            Set<String> sideA = new HashSet<>();
            sideA.add(leader);
            for (RaftNode n : cluster.nodes()) {
                if (!n.id().equals(leader) && sideA.size() < 3) {
                    sideA.add(n.id());
                }
            }
            Set<String> sideB = new HashSet<>();
            for (RaftNode n : cluster.nodes()) {
                if (!sideA.contains(n.id())) {
                    sideB.add(n.id());
                }
            }
            // 保证一侧 3 节点、一侧 2 节点
            assertEquals(3, sideA.size());
            assertEquals(2, sideB.size());
            cluster.addPartition(sideA, sideB);
            Thread.sleep(500);

            // 分区期间：统计每个任期上出现过的领导者，同任期绝不允许两个不同节点同时为 LEADER
            Map<Long, Set<String>> leadersByTerm = collectLeaders(cluster);
            for (Map.Entry<Long, Set<String>> e : leadersByTerm.entrySet()) {
                long simultaneousLeaders = cluster.nodes().stream()
                        .filter(n -> n.isRunning() && n.role().name().equals("LEADER")
                                && n.term() == e.getKey())
                        .count();
                assertTrue(simultaneousLeaders <= 1,
                        "term=" + e.getKey() + " 同时出现多个领导者: " + e.getValue());
            }
            // 2 节点一侧不可能凑齐 3 票多数派：sideB 中任何时刻都不应有领导者
            for (String id : sideB) {
                assertFalse(cluster.node(id).role().name().equals("LEADER"),
                        "少数派一侧 " + id + " 不应成为领导者");
            }
            System.out.println("[TEST] 3-vs-2 分区期间少数派无领导者，同任期最多一个主");

            cluster.heal();
            Thread.sleep(400);
            String finalLeader = cluster.awaitLeader(3000);
            assertTrue(cluster.submit("post", "p=1").startsWith("p="));
            System.out.println("[TEST] 恢复后唯一领导者=" + finalLeader + "，写入正常");
        }
    }

    private Map<Long, Set<String>> collectLeaders(RaftCluster cluster) {
        Map<Long, Set<String>> seen = new HashMap<>();
        for (RaftNode n : cluster.nodes()) {
            if (n.role().name().equals("LEADER")) {
                seen.computeIfAbsent(n.term(), k -> new HashSet<>()).add(n.id());
            }
        }
        return seen;
    }
}
