package com.github.lisama569.quorumreplicationlogjava.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.github.lisama569.quorumreplicationlogjava.raft.RaftCluster;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * 冒烟：3 节点启动后应选出唯一领导者，写入经多数派确认后法定人数读可见。
 */
class SmokeElectionTest {

    @Test
    void electOneLeader_andWriteIsVisible(@TempDir Path dir) {
        try (RaftCluster cluster = new RaftCluster(3, dir, 10)) {
            cluster.start();
            String leader = cluster.awaitLeader(3000);
            assertNotNull(leader);
            System.out.println("[TEST] 首轮领导者 = " + leader);

            cluster.submit("req-1", "color=blue");
            assertEquals("blue", cluster.readQuorum("color"));

            cluster.awaitConvergence(3000);
            for (var node : cluster.nodes()) {
                assertEquals("blue", node.readLocal("color"),
                        "节点 " + node.id() + " 收敛后应读到一致值");
            }
            System.out.println("[TEST] 三节点状态机已收敛 color=blue");
        }
    }
}
