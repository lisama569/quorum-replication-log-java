package com.github.lisama569.quorumreplicationlogjava.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.lisama569.quorumreplicationlogjava.raft.LogEntry;
import com.github.lisama569.quorumreplicationlogjava.raft.RaftCluster;
import com.github.lisama569.quorumreplicationlogjava.raft.RaftNode;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * 节点重启场景：
 * 1) 一个跟随者宕机期间集群继续写入，重启后从磁盘恢复任期/日志并补齐缺失条目；
 * 2) 领导者宕机后重启，集群选出新领导者，旧领导者恢复为跟随者且日志最终一致；
 * 3) 已确认写入（含宕机前多数派确认的）在任何重启组合后均不丢失。
 */
class RestartTest {

    @Test
    void followerCatchUp_afterRestart(@TempDir Path dir) throws Exception {
        try (RaftCluster cluster = new RaftCluster(5, dir, 10)) {
            cluster.start();
            String leader = cluster.awaitLeader(3000);
            cluster.submit("r0", "a=1");

            // 选一个跟随者宕机
            String down = cluster.nodes().stream().map(RaftNode::id)
                    .filter(id -> !id.equals(leader)).findFirst().orElseThrow();
            long downBefore = cluster.node(down).lastLogIndex();
            cluster.stopNode(down);
            System.out.println("[TEST] 跟随者 " + down + " 宕机 lastLogIndex=" + downBefore);

            // 宕机期间连续写入 5 条
            for (int i = 1; i <= 5; i++) {
                cluster.submit("r" + i, "k" + i + "=v" + i);
            }
            long leaderIndex = cluster.node(leader).lastLogIndex();
            System.out.println("[TEST] 宕机期间集群继续写入，领导者 lastLogIndex=" + leaderIndex);

            cluster.restartNode(down);
            System.out.println("[TEST] 节点 " + down + " 重启，等待心跳补齐日志");
            Thread.sleep(200);
            cluster.awaitConvergence(5000);

            RaftNode restarted = cluster.node(down);
            assertEquals(leaderIndex, restarted.lastLogIndex(), "重启节点必须补齐全部日志");
            assertEquals(leaderIndex, restarted.commitIndex());
            for (int i = 1; i <= 5; i++) {
                assertEquals("v" + i, restarted.readLocal("k" + i),
                        "重启节点状态机重放后应包含 k" + i);
            }
            // 校验日志序列与领导者完全一致（term/index/requestId/command）
            var leaderLog = cluster.node(leader).logEntries();
            var restartedLog = restarted.logEntries();
            assertEquals(leaderLog.size(), restartedLog.size());
            for (int i = 0; i < leaderLog.size(); i++) {
                LogEntry a = leaderLog.get(i);
                LogEntry b = restartedLog.get(i);
                assertEquals(a.term(), b.term(), "index=" + (i + 1) + " 任期一致");
                assertEquals(a.requestId(), b.requestId(), "index=" + (i + 1) + " 请求标识一致");
                assertEquals(a.command(), b.command(), "index=" + (i + 1) + " 命令一致");
            }
            System.out.println("[TEST] 重启节点补齐完成 lastLogIndex=" + restarted.lastLogIndex()
                    + " commitIndex=" + restarted.commitIndex() + " 判定依据=持久化日志+心跳重传");
        }
    }

    @Test
    void leaderRestart_clusterKeepsCommittedWrites(@TempDir Path dir) throws Exception {
        try (RaftCluster cluster = new RaftCluster(3, dir, 10)) {
            cluster.start();
            String leader = cluster.awaitLeader(3000);
            cluster.submit("w1", "color=red");
            cluster.submit("w2", "shape=circle");
            long committedIndex = cluster.node(leader).commitIndex();

            // 领导者宕机
            cluster.stopNode(leader);
            System.out.println("[TEST] 领导者 " + leader + " 宕机 commitIndex=" + committedIndex);
            String newLeader = null;
            long deadline = System.currentTimeMillis() + 3000;
            while (System.currentTimeMillis() < deadline) {
                var leaders = cluster.nodes().stream()
                        .filter(n -> n.isRunning() && n.role().name().equals("LEADER")).toList();
                if (leaders.size() == 1 && !leaders.get(0).id().equals(leader)) {
                    newLeader = leaders.get(0).id();
                    break;
                }
                Thread.sleep(15);
            }
            assertTrue(newLeader != null, "剩余多数派必须选出新领导者");
            System.out.println("[TEST] 新领导者 = " + newLeader);

            // 新任期继续写入
            cluster.node(newLeader).submit("w3", "size=large");
            assertEquals("red", cluster.node(newLeader).readQuorum("color"));

            // 旧领导者重启，补齐新任期日志
            cluster.restartNode(leader);
            Thread.sleep(300);
            cluster.awaitConvergence(5000);

            for (var n : cluster.nodes()) {
                assertEquals("red", n.readLocal("color"), "旧写入 color=red 不丢失，节点=" + n.id());
                assertEquals("circle", n.readLocal("shape"), "shape=circle 不丢失，节点=" + n.id());
                assertEquals("large", n.readLocal("size"), "新写入 size=large 应收敛，节点=" + n.id());
            }
            System.out.println("[TEST] 旧领导者重启后三节点收敛，所有已确认写入均未丢失");
        }
    }
}
