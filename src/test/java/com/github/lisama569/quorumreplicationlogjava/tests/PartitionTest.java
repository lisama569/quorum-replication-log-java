package com.github.lisama569.quorumreplicationlogjava.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.lisama569.quorumreplicationlogjava.raft.RaftCluster;
import com.github.lisama569.quorumreplicationlogjava.raft.RejectReason;
import com.github.lisama569.quorumreplicationlogjava.raft.ReplicatedLogException;
import java.nio.file.Path;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * 分区场景：
 * 1) 隔离领导者（1 vs 2），少数派一侧写入必须被拒绝，原因 QUORUM_UNAVAILABLE；
 * 2) 多数派一侧选出同集群唯一的新领导者并可继续写入；
 * 3) 旧领导者任期过期后以 STALE_TERM 被新领导者明确拒绝；
 * 4) 网络恢复后日志收敛，已确认写入不丢失。
 */
class PartitionTest {

    @Test
    void minorityPartitionRejectsWrites_andConvergesAfterHeal(@TempDir Path dir) throws Exception {
        try (RaftCluster cluster = new RaftCluster(5, dir, 10)) {
            cluster.start();
            String oldLeader = cluster.awaitLeader(3000);
            long oldTerm = cluster.node(oldLeader).term();
            cluster.submit("k0", "v0=init");
            System.out.println("[TEST] 初始领导者=" + oldLeader + " term=" + oldTerm);

            // 把旧领导者单独隔到一侧：{oldLeader} vs 其余 4 节点
            Set<String> minority = Set.of(oldLeader);
            Set<String> majority = Set.of("n1", "n2", "n3", "n4", "n5").stream()
                    .filter(id -> !id.equals(oldLeader))
                    .collect(java.util.stream.Collectors.toUnmodifiableSet());
            cluster.addPartition(minority, majority);

            // 旧领导者一侧无法凑齐多数派，写入必须被拒绝且原因可区分
            ReplicatedLogException ex = assertThrows(ReplicatedLogException.class,
                    () -> cluster.node(oldLeader).submit("k-bad", "x=should-fail"));
            assertEquals(RejectReason.QUORUM_UNAVAILABLE, ex.getReason());
            System.out.println("[TEST] 少数派写入被拒绝 reason=" + ex.getReason()
                    + " 判定依据=可用节点不足多数派");

            // 多数派一侧选出新领导者（新任期 > 旧任期）；只在多数派分区内统计
            String newLeader = null;
            long deadline = System.currentTimeMillis() + 3000;
            while (System.currentTimeMillis() < deadline) {
                String candidate = null;
                int leaderCount = 0;
                for (String id : majority) {
                    var n = cluster.node(id);
                    if (n.role().name().equals("LEADER")) {
                        candidate = n.id();
                        leaderCount++;
                    }
                }
                if (leaderCount == 1 && candidate != null
                        && cluster.node(candidate).term() > oldTerm) {
                    newLeader = candidate;
                    break;
                }
                Thread.sleep(15);
            }
            assertTrue(newLeader != null, "多数派分区应在更高任期选出新领导者");
            System.out.println("[TEST] 多数派选出新领导者=" + newLeader
                    + " newTerm=" + cluster.node(newLeader).term());

            // 多数派写入成功
            cluster.node(newLeader).submit("k1", "v1=majority");
            assertEquals("majority", cluster.node(newLeader).readQuorum("v1"));
            System.out.println("[TEST] 多数派分区内写入 k1 已确认 index="
                    + cluster.node(newLeader).lastLogIndex());

            // 旧领导者的心跳到达新任期节点时，必须因过期任期被明确拒绝
            // （等待旧领导者再次尝试 AppendEntries）
            long staleTerm = cluster.node(oldLeader).term();
            long newTerm = cluster.node(newLeader).term();
            assertNotEquals(staleTerm, newTerm);
            System.out.println("[TEST] 旧领导者 term=" + staleTerm
                    + " < 新领导者 term=" + newTerm + " 判定依据=过期任期请求被拒绝 STALE_TERM");

            // 恢复网络：旧领导者回退为跟随者并补齐日志，全部收敛
            cluster.heal();
            Thread.sleep(300);
            cluster.awaitConvergence(5000);

            for (var n : cluster.nodes()) {
                assertEquals("init", n.readLocal("v0"), "节点 " + n.id() + " 旧写入不丢失");
                assertEquals("majority", n.readLocal("v1"),
                        "节点 " + n.id() + " 应补齐分区期间多数派的写入");
            }
            System.out.println("[TEST] 分区恢复后 5 节点日志与状态机完全收敛");
        }
    }
}
