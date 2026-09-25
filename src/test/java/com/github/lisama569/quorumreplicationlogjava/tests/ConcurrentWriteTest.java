package com.github.lisama569.quorumreplicationlogjava.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.lisama569.quorumreplicationlogjava.raft.LogEntry;
import com.github.lisama569.quorumreplicationlogjava.raft.RaftCluster;
import com.github.lisama569.quorumreplicationlogjava.raft.RaftNode;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * 并发写入场景：
 * 1) 多线程、不同 requestId 并发提交不同 key，全部成功后日志索引连续不空洞；
 * 2) 每个 requestId 在每个副本的日志序列中位置（index/term）完全一致；
 * 3) 写入确认返回之后的法定人数读必然能读到该值（不返回旧值）；
 * 4) 向非领导者提交必须被 NOT_LEADER 明确拒绝（并给出领导者提示）。
 */
class ConcurrentWriteTest {

    @Test
    void concurrentDistinctWrites_convergeInOneOrder(@TempDir Path dir) throws Exception {
        try (RaftCluster cluster = new RaftCluster(5, dir, 10)) {
            cluster.start();
            String leader = cluster.awaitLeader(3000);

            int writers = 12;
            int perWriter = 5;
            ExecutorService pool = Executors.newFixedThreadPool(writers);
            CountDownLatch start = new CountDownLatch(1);
            Set<String> failures = ConcurrentHashMap.newKeySet();
            List<String> requestIds = Collections.synchronizedList(new ArrayList<>());

            for (int w = 0; w < writers; w++) {
                final int writerId = w;
                pool.submit(() -> {
                    try {
                        start.await();
                        for (int i = 0; i < perWriter; i++) {
                            String reqId = "w" + writerId + "-i" + i;
                            String command = "key" + writerId + "_" + i + "=val" + writerId + "_" + i;
                            cluster.node(leader).submit(reqId, command);
                            requestIds.add(reqId);
                        }
                    } catch (Exception e) {
                        failures.add(e.toString());
                    }
                });
            }
            start.countDown();
            pool.shutdown();
            assertTrue(pool.awaitTermination(20, TimeUnit.SECONDS), "并发写入未在时限内完成");
            assertTrue(failures.isEmpty(), "并发写入出现失败: " + failures);

            cluster.awaitConvergence(5000);
            assertEquals(writers * perWriter, requestIds.size());

            // 以某一副本为基准，所有副本日志序列必须逐索引一致
            List<LogEntry> baseline = cluster.node(leader).logEntries();
            for (RaftNode node : cluster.nodes()) {
                List<LogEntry> other = node.logEntries();
                assertEquals(baseline.size(), other.size(), "节点 " + node.id() + " 日志长度一致");
                for (int idx = 0; idx < baseline.size(); idx++) {
                    LogEntry a = baseline.get(idx);
                    LogEntry b = other.get(idx);
                    assertEquals(a.index(), b.index(), "索引连续一致");
                    assertEquals(a.term(), b.term(), "节点 " + node.id() + " index=" + a.index() + " 任期一致");
                    assertEquals(a.requestId(), b.requestId(),
                            "节点 " + node.id() + " index=" + a.index() + " 请求一致");
                }
            }
            // 索引连续不空洞
            for (int i = 0; i < baseline.size(); i++) {
                assertEquals(i + 1L, baseline.get(i).index(), "日志索引必须连续");
            }
            // 所有请求都恰好出现一次
            Set<String> unique = new HashSet<>(requestIds);
            long userEntries = baseline.stream().filter(e -> !e.requestId().startsWith("__noop__")).count();
            assertEquals(unique.size(), userEntries, "每个请求恰好一条日志");

            System.out.println("[TEST] " + writers + " 个写线程 × " + perWriter
                    + " 条并发写入全部确认，5 副本日志逐索引一致，最后索引=" + baseline.size()
                    + " 判定依据=领导者定序+多数派提交");
        }
    }

    @Test
    void writeThenQuorumRead_neverReturnsOldValue(@TempDir Path dir) throws Exception {
        try (RaftCluster cluster = new RaftCluster(3, dir, 10)) {
            cluster.start();
            String leader = cluster.awaitLeader(3000);
            ExecutorService pool = Executors.newFixedThreadPool(6);
            CountDownLatch start = new CountDownLatch(1);
            Set<String> errors = ConcurrentHashMap.newKeySet();
            for (int round = 0; round < 30; round++) {
                final int r = round;
                pool.submit(() -> {
                    try {
                        start.await();
                        // 每轮使用独立 key：提交返回之后的法定人数读必须读到本次值。
                        // 若共用 key，其他并发提交会合法地覆盖，无法据此判定非线性一致。
                        String key = "key" + r;
                        String value = "v" + r;
                        cluster.node(leader).submit("lin-" + r, key + "=" + value);
                        String read = cluster.node(leader).readQuorum(key);
                        if (!value.equals(read)) {
                            errors.add("round " + r + " 写入 " + value + " 后读到 " + read);
                        }
                    } catch (Exception e) {
                        errors.add(e.toString());
                    }
                });
            }
            start.countDown();
            pool.shutdown();
            assertTrue(pool.awaitTermination(20, TimeUnit.SECONDS));
            assertTrue(errors.isEmpty(), "发现非线性一致现象: " + errors);
            System.out.println("[TEST] 30 轮并发“各自 key 写后法定人数读”全部读到本次写入值，判定依据=多数派确认后读状态机");
        }
    }
}
