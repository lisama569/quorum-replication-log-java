package com.github.lisama569.quorumreplicationlogjava.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.github.lisama569.quorumreplicationlogjava.raft.LogEntry;
import com.github.lisama569.quorumreplicationlogjava.raft.RaftCluster;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * 幂等重试场景：
 * 1) 相同 requestId 串行重复提交，只入一条日志、状态机只应用一次；
 * 2) 相同 requestId 被多线程并发提交，所有线程得到同一结果，日志仍只有一条；
 * 3) 不同 requestId 携带相同命令也会各自成条目（幂等键是 requestId 而非命令内容）。
 */
class IdempotencyTest {

    private long countOfRequest(List<LogEntry> entries, String requestId) {
        return entries.stream().filter(e -> e.requestId().equals(requestId)).count();
    }

    @Test
    void duplicateRequestId_isAppliedOnce(@TempDir Path dir) {
        try (RaftCluster cluster = new RaftCluster(3, dir, 10)) {
            cluster.start();
            cluster.awaitLeader(3000);

            for (int i = 0; i < 5; i++) {
                String result = cluster.submit("dup-1", "counter=100");
                assertEquals("counter=100", result, "重复提交应返回首次结果");
            }
            cluster.awaitConvergence(3000);

            for (var node : cluster.nodes()) {
                assertEquals(1L, countOfRequest(node.logEntries(), "dup-1"),
                        "节点 " + node.id() + " 日志中 requestId=dup-1 只能出现一次");
                assertEquals("100", node.readLocal("counter"),
                        "节点 " + node.id() + " 状态机值不应被重复应用改变");
            }
            System.out.println("[TEST] 串行重复提交 5 次 requestId=dup-1：日志仅 1 条，判定依据=请求标识去重");
        }
    }

    @Test
    void concurrentDuplicateRequestId_singleLogEntry(@TempDir Path dir) throws Exception {
        try (RaftCluster cluster = new RaftCluster(3, dir, 10)) {
            cluster.start();
            String leader = cluster.awaitLeader(3000);

            int threads = 8;
            ExecutorService pool = Executors.newFixedThreadPool(threads);
            CountDownLatch start = new CountDownLatch(1);
            CountDownLatch done = new CountDownLatch(threads);
            for (int t = 0; t < threads; t++) {
                pool.submit(() -> {
                    try {
                        start.await();
                        String r = cluster.node(leader).submit("race-1", "race=win");
                        if (!"race=win".equals(r)) {
                            throw new AssertionError("并发重复提交结果异常: " + r);
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    } finally {
                        done.countDown();
                    }
                });
            }
            start.countDown();
            boolean finished = done.await(10, TimeUnit.SECONDS);
            pool.shutdownNow();
            if (!finished) {
                throw new AssertionError("并发重复提交未能在时限内完成（可能存在重复复制或唤醒缺失）");
            }
            cluster.awaitConvergence(3000);

            for (var node : cluster.nodes()) {
                assertEquals(1L, countOfRequest(node.logEntries(), "race-1"),
                        "节点 " + node.id() + " 并发提交后 requestId=race-1 仍只能有一条日志");
                assertEquals("win", node.readLocal("race"));
            }
            System.out.println("[TEST] " + threads + " 线程并发提交同一 requestId=race-1：日志仅 1 条，全部成功");
        }
    }

    @Test
    void sameCommandDifferentRequestId_separateEntries(@TempDir Path dir) {
        try (RaftCluster cluster = new RaftCluster(3, dir, 10)) {
            cluster.start();
            cluster.awaitLeader(3000);
            cluster.submit("a-1", "x=1");
            cluster.submit("a-2", "x=1");
            cluster.awaitConvergence(3000);
            assertEquals(1L, countOfRequest(cluster.node("n1").logEntries(), "a-1"));
            assertEquals(1L, countOfRequest(cluster.node("n1").logEntries(), "a-2"));
            System.out.println("[TEST] 相同命令、不同 requestId 各自成条目，判定依据=幂等键为请求标识");
        }
    }
}
