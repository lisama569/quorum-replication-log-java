package com.github.lisama569.quorumreplicationlogjava.tests;

import com.github.lisama569.quorumreplicationlogjava.replication.Cluster;
import com.github.lisama569.quorumreplicationlogjava.replication.LogEntry;
import com.github.lisama569.quorumreplicationlogjava.replication.ReadResult;
import com.github.lisama569.quorumreplicationlogjava.replication.WriteResult;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 并发读写 + 副本追赶窗口下，读的零副作用与线性一致验证。
 *
 * <p>场景：一个写线程持续写入，多个读线程持续读；另有一个副本在分区/恢复
 * 之间反复抖动。任何时刻读都不得给抖动副本或其他副本补日志/提提交位置——
 * 该副本只能通过显式 {@code replicate()} 追赶。
 */
class ConcurrentReadNoSideEffectTest {

    private static final Logger log = LoggerFactory.getLogger(ConcurrentReadNoSideEffectTest.class);

    @Test
    void concurrentReadsNeverReplicateAndStayLinearizable() throws Exception {
        Cluster c = new Cluster(List.of("n1", "n2", "n3", "n4", "n5"));
        assertTrue(c.elect("n1"));
        assertTrue(c.write("n1", "seed", "v0").accepted());
        c.replicate("n1");
        long term = c.node("n1").getCurrentTerm();

        int writes = 30;
        int readers = 4;
        ExecutorService pool = Executors.newFixedThreadPool(readers + 2);
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger writesDone = new AtomicInteger();
        ConcurrentLinkedQueue<String> anomalies = new ConcurrentLinkedQueue<>();
        ConcurrentLinkedQueue<ReadResult> failedReads = new ConcurrentLinkedQueue<>();

        // 分区线程：把 n4 周期性隔离/恢复。恢复窗口内不调用 replicate()，
        // 因此 n4 在每次“刚恢复”时必然落后于多数派——旧实现下读确认捎带的
        // 日志/leaderCommit 会在这个窗口推进 n4；正确实现下 n4 完全冻结。
        Thread partition = new Thread(() -> {
            boolean isolated = false;
            while (writesDone.get() < writes) {
                if (isolated) {
                    c.heal("n4");
                } else {
                    c.isolate("n4");
                }
                isolated = !isolated;
                try {
                    Thread.sleep(3);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
            c.heal("n4");
        }, "partition-n4");

        pool.submit(() -> {
            try {
                start.await();
                partition.start();
                for (int i = 0; i < writes; i++) {
                    WriteResult r = c.write("n1", "w" + i, "v" + (i + 1));
                    if (!r.accepted()) {
                        anomalies.add("写入 w" + i + " 被拒绝: " + r.reason());
                        return;
                    }
                    writesDone.incrementAndGet();
                    c.replicate("n1"); // 只有真正的复制推进 n2/n3（n4 抖动中常常收不到）
                    Thread.sleep(1); // 给读线程与分区线程留出交错窗口
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        for (int t = 0; t < readers; t++) {
            final int rid = t;
            pool.submit(() -> {
                try {
                    start.await();
                    long lastReadIndex = 0;
                    while (writesDone.get() < writes) {
                        ReadResult r = c.read("n1");
                        if (!r.success()) {
                            failedReads.add(r);
                            continue;
                        }
                        List<LogEntry> entries = r.entries();
                        for (int i = 0; i < entries.size(); i++) {
                            if (entries.get(i).index() != i + 1) {
                                anomalies.add("读者" + rid + " 读到非连续前缀: pos=" + (i + 1)
                                        + " entry=" + entries.get(i).index());
                            }
                        }
                        if (entries.size() != r.readIndex()) {
                            anomalies.add("读者" + rid + " entries=" + entries.size()
                                    + " 与 readIndex=" + r.readIndex() + " 不符");
                        }
                        if (r.readIndex() < lastReadIndex) {
                            anomalies.add("读者" + rid + " 读位置回退: " + lastReadIndex + " -> " + r.readIndex());
                        }
                        if (r.term() != term) {
                            anomalies.add("读者" + rid + " 读到非预期任期: " + r.term());
                        }
                        lastReadIndex = r.readIndex();
                    }
                    log.info("[read] 读者{} 完成，最终 readIndex={}", rid, lastReadIndex);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }

        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(60, TimeUnit.SECONDS));
        partition.join(5000);

        assertTrue(anomalies.isEmpty(), () -> "并发读写一致性问题: " + new ArrayList<>(anomalies));
        assertTrue(failedReads.isEmpty(),
                () -> "多数派(n1,n2,n3,n5)始终可用，读不应失败: "
                        + failedReads.stream().map(ReadResult::reason).distinct().toList());

        // ---- 第二阶段（确定性窗口）：n4 落后 -> 隔离状态下多数派再写入 ->
        //      n4 恢复连通但不复制 -> 读风暴。旧实现此时必然借读给 n4 补日志/提提交位置。
        c.isolate("n4");
        int frozenSize = c.node("n4").getLog().size();
        int extra = 10;
        for (int i = 0; i < extra; i++) {
            WriteResult r = c.write("n1", "x" + i, "x" + i);
            assertTrue(r.accepted(), "n1,n2,n3,n5 仍构成 5 节点多数派");
            // 只向 n4 以外的副本做复制推进，保证 n4 恢复连通时确实落后
            c.node("n1").replicate(c, List.of("n2", "n3", "n5"));
        }
        int total = writes + 1 + extra;
        assertEquals(total, c.node("n1").getCommitIndex());
        assertEquals(frozenSize, c.node("n4").getLog().size(), "隔离期间 n4 不应收到任何复制");

        c.heal("n4"); // 恢复连通；注意不调用 replicate()
        // 让 n2/n3 暂时不可达：5 节点多数派=3，领导者 n1 + n5 + 刚恢复的 n4
        // 恰好凑齐——读确认流量必然打到落后的 n4（旧实现捎带的 leaderCommit
        // 会把 n4 提交位置前推；n5 日志完整，捎不带新条目）。
        c.disconnect("n1", "n2");
        c.disconnect("n1", "n3");
        int n4LogBeforeReadStorm = c.node("n4").getLog().size();
        long n4CommitBeforeReadStorm = c.node("n4").getCommitIndex();
        assertTrue(n4LogBeforeReadStorm < total,
                "n4 必须落后于多数派才能构成追赶窗口: logSize=" + n4LogBeforeReadStorm + " < " + total);
        // n4 本地日志的最后一条已提交：旧实现读确认捎带 leaderCommit 会把它的
        // commitIndex 凭空推到领导者位置；正确实现下提交位置必须冻结
        assertEquals(n4LogBeforeReadStorm, n4CommitBeforeReadStorm,
                "隔离前 n4 已有日志的提交位置应等于其日志末尾");
        log.info("[case] 确定性追赶窗口：n4 恢复连通但未复制，logSize={} commitIndex={}，多数派已到 {}",
                n4LogBeforeReadStorm, n4CommitBeforeReadStorm, total);

        for (int i = 0; i < 30; i++) {
            ReadResult r = c.read("n1");
            assertTrue(r.success(), "n1+n4+n5 应凑齐多数派确认");
        }
        assertEquals(n4LogBeforeReadStorm, c.node("n4").getLog().size(),
                "30 次读不得给刚恢复的落后副本补任何日志条目");
        assertEquals(n4CommitBeforeReadStorm, c.node("n4").getCommitIndex(),
                "30 次读不得推进落后副本提交位置");
        log.info("[assert] 追赶窗口读风暴后 n4 冻结在 logSize={} commitIndex={}，依据=读确认不带条目/leaderCommit",
                n4LogBeforeReadStorm, n4CommitBeforeReadStorm);

        // 恢复全连通
        c.connect("n1", "n2");
        c.connect("n1", "n3");

        // 最终读：多数派已确认全部写入，读到的必须是连续前缀
        ReadResult finalRead = c.read("n1");
        assertTrue(finalRead.success());
        assertEquals(total, finalRead.readIndex());
        assertEquals(term, finalRead.term());

        // 多数派内各副本：日志都已含全部写入；提交位置只由真实复制推进
        for (String id : List.of("n1", "n2", "n3", "n5")) {
            assertEquals(total, c.node(id).getLog().size(), id + " 日志数应等于写入数");
            assertTrue(c.node(id).getCommitIndex() >= total - 1,
                    id + " 提交位置最多落后一轮心跳，实际=" + c.node(id).getCommitIndex());
        }

        // 对照组：只有真正的复制能让 n4 收敛
        c.replicate("n1");
        c.replicate("n1");
        assertEquals(total, c.node("n4").getCommitIndex());
        List<String> got = c.node("n4").getCommittedEntries().stream().map(LogEntry::payload).toList();
        assertEquals(finalRead.entries().stream().map(LogEntry::payload).toList(), got);
        log.info("[assert] 对照组：replicate() 后 n4 收敛到 readIndex={}，日志序列与读结果逐条一致",
                total);

        // 领导者日志条目数恰好等于写入数：读从未产生条目
        assertEquals(total, c.node("n1").getLog().size());
        log.info("[state] node=n1 role={} term={} commitIndex={} logSize={} | 最终",
                c.node("n1").getRole(), c.node("n1").getCurrentTerm(),
                c.node("n1").getCommitIndex(), c.node("n1").getLog().size());
    }
}
