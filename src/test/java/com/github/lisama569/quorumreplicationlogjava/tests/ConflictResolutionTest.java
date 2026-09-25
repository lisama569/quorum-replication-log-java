package com.github.lisama569.quorumreplicationlogjava.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.lisama569.quorumreplicationlogjava.raft.AppendEntriesRequest;
import com.github.lisama569.quorumreplicationlogjava.raft.LogEntry;
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
 * 日志冲突解决：
 *
 * <p>构造一个带有未提交分叉条目的旧领导者：先让它在隔离前只把某条写入复制到自己
 * （未达多数派，未提交），随后多数派另选新领导并继续提交。旧领导者恢复后，
 * 必须在与新领导者日志首次冲突的任期/索引处截断未提交分叉，再追加成一致序列；
 * 而分区前已确认的条目绝不能被删除。
 */
class ConflictResolutionTest {

    @Test
    void uncommittedBranchIsTruncated_committedEntriesSurvive(@TempDir Path dir) throws Exception {
        try (RaftCluster cluster = new RaftCluster(5, dir, 10)) {
            cluster.start();
            String leader = cluster.awaitLeader(3000);

            // 已确认基线：3 条用户写入（各自独立 key）
            cluster.submit("c1", "base1=v1");
            cluster.submit("c2", "base2=v2");
            cluster.submit("c3", "base3=v3");
            cluster.awaitConvergence(3000);
            long committedIndex = cluster.node(leader).commitIndex();
            System.out.println("[TEST] 基线已确认 commitIndex=" + committedIndex);

            // 隔离旧领导者单独一侧（1 vs 4），使其任何后续写入都无法达多数派
            Set<String> others = new java.util.HashSet<>();
            for (RaftNode n : cluster.nodes()) {
                if (!n.id().equals(leader)) {
                    others.add(n.id());
                }
            }
            cluster.addPartition(Set.of(leader), others);

            // 模拟旧领导者本地已追加但从未达多数派的“未提交分叉”（论文 Figure 8 场景）：
            // 分叉使用旧领导者自己的当前任期；多数派恢复后会在更高任期覆盖该索引。
            long branchIndex = committedIndex + 1;
            long branchTerm = cluster.node(leader).term();
            LogEntry branchEntry = new LogEntry(branchTerm, branchIndex, "branch-1", "ghost=zzz");
            cluster.node(leader).forceAppendForFaultInjection(List.of(branchEntry));
            assertEquals(branchIndex, cluster.node(leader).lastLogIndex());
            System.out.println("[TEST] 旧领导者本地存在未提交分叉 index=" + branchIndex
                    + " term=" + branchTerm + " 判定依据=该条目从未获多数派复制，不属于已确认日志");

            // 隔离期间旧领导者的真实写入必须在入口被拒（QUORUM_UNAVAILABLE），不会新增分叉
            ReplicatedLogException rejected = org.junit.jupiter.api.Assertions.assertThrows(
                    ReplicatedLogException.class,
                    () -> cluster.node(leader).submit("branch-2", "ghost2=yyy"));
            assertEquals(RejectReason.QUORUM_UNAVAILABLE, rejected.getReason());

            // 多数派一侧选出新领导者并提交新条目
            String newLeader = null;
            long deadline = System.currentTimeMillis() + 3000;
            while (System.currentTimeMillis() < deadline) {
                for (String id : others) {
                    if (cluster.node(id).role().name().equals("LEADER")
                            && cluster.node(id).term() > cluster.node(leader).term()) {
                        newLeader = id;
                    }
                }
                if (newLeader != null) {
                    break;
                }
                Thread.sleep(15);
            }
            assertTrue(newLeader != null, "多数派必须选出新领导者");
            cluster.node(newLeader).submit("c4", "after4=v4");
            cluster.node(newLeader).submit("c5", "after5=v5");
            System.out.println("[TEST] 新领导者 " + newLeader + " 在分叉之后又确认两条");

            // 恢复网络，等待收敛
            cluster.heal();
            Thread.sleep(300);
            cluster.awaitConvergence(5000);

            // 所有节点日志必须逐索引一致，且不存在分叉请求
            for (RaftNode n : cluster.nodes()) {
                assertEquals(cluster.node(newLeader).lastLogIndex(), n.lastLogIndex(),
                        "节点 " + n.id() + " 日志长度必须收敛");
                long ghostCount = n.logEntries().stream()
                        .filter(e -> e.requestId().equals("branch-1")).count();
                assertEquals(0L, ghostCount,
                        "节点 " + n.id() + " 上的未提交分叉必须被冲突截断规则删除");
                // 已确认基线仍在
                assertEquals("v1", n.readLocal("base1"), "节点 " + n.id() + " 已确认写入不丢失");
                assertEquals("v2", n.readLocal("base2"), "节点 " + n.id() + " 已确认写入不丢失");
                assertEquals("v3", n.readLocal("base3"), "节点 " + n.id() + " 已确认写入不丢失");
                assertEquals("v4", n.readLocal("after4"), "节点 " + n.id() + " 新任期写入收敛");
                assertEquals("v5", n.readLocal("after5"), "节点 " + n.id() + " 新任期写入收敛");
                // 已确认区段（索引 <= committedIndex）与原领导者完全相同
                for (long idx = 1; idx <= committedIndex; idx++) {
                    LogEntry a = cluster.node(newLeader).logEntries().get((int) idx - 1);
                    LogEntry b = n.logEntries().get((int) idx - 1);
                    assertEquals(a.requestId(), b.requestId(),
                            "已确认索引 " + idx + " 在节点 " + n.id() + " 上不得改变");
                }
            }
            System.out.println("[TEST] 冲突解决验证通过：未提交分叉被截断，commitIndex="
                    + committedIndex + " 及之前的已确认条目全部保留");
        }
    }
}
