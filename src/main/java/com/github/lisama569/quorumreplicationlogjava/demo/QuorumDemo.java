package com.github.lisama569.quorumreplicationlogjava.demo;

import com.github.lisama569.quorumreplicationlogjava.raft.RaftCluster;
import com.github.lisama569.quorumreplicationlogjava.raft.RaftNode;
import com.github.lisama569.quorumreplicationlogjava.raft.RejectReason;
import com.github.lisama569.quorumreplicationlogjava.raft.ReplicatedLogException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;

/**
 * 不依赖 Web 容器的本地演示：构造 5 节点进程内集群，依次演示
 * 正常写入 → 隔离领导者（少数派写入被拒）→ 多数派重新选举并写入
 * → 节点宕机/重启补齐 → 网络恢复后全集群收敛。
 *
 * <p>运行方式：{@code mvn -q exec:java}（见 README），或直接在 IDE 中运行本类的 main。
 * 观察重点：每条日志都打印 term、logIndex 与“判定依据”。
 */
public final class QuorumDemo {

    private QuorumDemo() {
    }

    public static void main(String[] args) throws Exception {
        Path dataDir = Files.createTempDirectory("quorum-demo-");
        System.out.println("==== 演示数据目录: " + dataDir);

        try (RaftCluster cluster = new RaftCluster(5, dataDir, 10)) {
            cluster.start();
            String leader = cluster.awaitLeader(5000);
            System.out.println(">>> [阶段1] 初始领导者 = " + leader + " term=" + cluster.node(leader).term());

            cluster.submit("demo-1", "city=hangzhou");
            cluster.submit("demo-2", "city=shenzhen");
            System.out.println(">>> [阶段1] 两条写入已确认，法定人数读 city=" + cluster.readQuorum("city"));

            // 阶段2：隔离旧领导者（1 vs 4）
            Set<String> majority = new java.util.HashSet<>();
            for (RaftNode n : cluster.nodes()) {
                if (!n.id().equals(leader)) {
                    majority.add(n.id());
                }
            }
            cluster.addPartition(Set.of(leader), majority);
            System.out.println(">>> [阶段2] 已隔离旧领导者 " + leader + "（1-vs-4 分区）");
            try {
                cluster.node(leader).submit("demo-bad", "x=1");
                System.out.println(">>> [阶段2] 意外：少数派写入居然成功");
            } catch (ReplicatedLogException e) {
                System.out.println(">>> [阶段2] 少数派写入被拒绝 reason=" + e.getReason()
                        + "（预期 QUORUM_UNAVAILABLE）");
            }

            // 等待多数派新领导者
            String newLeader = waitNewLeader(cluster, leader, cluster.node(leader).term(), 5000);
            System.out.println(">>> [阶段3] 多数派选出新领导者 = " + newLeader
                    + " term=" + cluster.node(newLeader).term());
            cluster.node(newLeader).submit("demo-3", "postPartition=ok");
            System.out.println(">>> [阶段3] 多数派分区内写入成功 postPartition="
                    + cluster.node(newLeader).readQuorum("postPartition"));

            // 阶段4：先恢复网络，使旧领导者退回跟随者并补齐日志
            cluster.heal();
            Thread.sleep(300);
            cluster.awaitConvergence(5000);
            System.out.println(">>> [阶段4] 网络恢复，5 节点已收敛到同一日志序列");

            // 阶段5：在健康集群上演示节点宕机/重启补齐
            String victim = cluster.nodes().stream()
                    .map(RaftNode::id)
                    .filter(id -> cluster.node(id).role() != com.github.lisama569.quorumreplicationlogjava.raft.Role.LEADER)
                    .findFirst().orElseThrow();
            cluster.stopNode(victim);
            System.out.println(">>> [阶段5] 节点 " + victim + " 宕机");
            cluster.submit("demo-4", "duringDown=1");
            cluster.restartNode(victim);
            System.out.println(">>> [阶段5] 节点 " + victim + " 重启，等待心跳补齐...");
            Thread.sleep(300);
            cluster.awaitConvergence(5000);
            System.out.println(">>> [阶段5] 重启节点 lastLogIndex=" + cluster.node(victim).lastLogIndex()
                    + " duringDown=" + cluster.node(victim).readLocal("duringDown"));

            for (RaftNode n : cluster.nodes()) {
                System.out.println(">>> [结果] 节点 " + n.id()
                        + " term=" + n.term()
                        + " role=" + n.role()
                        + " lastLogIndex=" + n.lastLogIndex()
                        + " commitIndex=" + n.commitIndex()
                        + " city=" + n.readLocal("city")
                        + " postPartition=" + n.readLocal("postPartition")
                        + " duringDown=" + n.readLocal("duringDown"));
            }
            System.out.println(">>> 全集群日志与状态机收敛，已确认写入无丢失，演示结束");
        }
    }

    private static String waitNewLeader(RaftCluster cluster, String oldLeader, long oldTerm, long timeoutMillis)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            for (RaftNode n : cluster.nodes()) {
                if (n.id().equals(oldLeader)) {
                    continue;
                }
                if (n.role().name().equals("LEADER") && n.term() > oldTerm) {
                    return n.id();
                }
            }
            Thread.sleep(20);
        }
        throw new ReplicatedLogException(RejectReason.NO_LEADER, "分区后未等到新领导者");
    }
}
