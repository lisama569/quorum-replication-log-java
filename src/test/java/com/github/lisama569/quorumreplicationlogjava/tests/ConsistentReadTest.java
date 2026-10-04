package com.github.lisama569.quorumreplicationlogjava.tests;

import com.github.lisama569.quorumreplicationlogjava.replication.Cluster;
import com.github.lisama569.quorumreplicationlogjava.replication.LogEntry;
import com.github.lisama569.quorumreplicationlogjava.replication.NodeRole;
import com.github.lisama569.quorumreplicationlogjava.replication.ReadResult;
import com.github.lisama569.quorumreplicationlogjava.replication.RejectionReason;
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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 一致性读（ReadIndex 风格）的行为验证：写后立读、旧领导者隔离、
 * 多数派不可达/确认超时、选举窗口、并发读写。日志打印任期、读到的
 * 日志位置（readIndex）与判定依据。
 */
class ConsistentReadTest {

    private static final Logger log = LoggerFactory.getLogger(ConsistentReadTest.class);
    private static final List<String> IDS = List.of("n1", "n2", "n3");

    private Cluster newCluster() {
        return new Cluster(IDS);
    }

    private void describe(String fmt, Object... args) {
        log.info("[case] " + fmt, args);
    }

    private void logRead(String who, ReadResult r, String why) {
        if (r.success()) {
            log.info("[read] node={} 成功 term={} readIndex={} entries={} | {}",
                    who, r.term(), r.readIndex(),
                    r.entries().stream().map(e -> e.index() + "@t" + e.term() + ":" + e.payload()).toList(),
                    why);
        } else {
            log.info("[read] node={} 失败 reason={} leaderHint={} | {}", who, r.reason(), r.leaderId(), why);
        }
    }

    // ----------------------------------------------------------------
    // 写完之后马上读
    // ----------------------------------------------------------------

    @Test
    void writeThenReadSeesCommittedWriteAndReportsPosition() {
        Cluster c = newCluster();
        assertTrue(c.elect("n1"));
        long term = c.node("n1").getCurrentTerm();

        WriteResult w1 = c.write("n1", "r1", "v1");
        assertTrue(w1.accepted());
        // 写确认后立刻读：必须看得到该写入
        ReadResult r1 = c.read("n1");
        logRead("n1", r1, "写入 r1 确认后立即读");
        assertTrue(r1.success());
        assertEquals(term, r1.term(), "读结果应报告确认读时的任期");
        assertTrue(r1.readIndex() >= w1.index(),
                () -> "读位置 readIndex=" + r1.readIndex() + " 应覆盖已确认写入 index=" + w1.index());
        assertEquals(List.of("v1"), r1.entries().stream().map(LogEntry::payload).toList());
        log.info("[assert] 写后立读：readIndex={} >= 写入 index={}，依据=读以多数派确认后的 commitIndex 为上界",
                r1.readIndex(), w1.index());

        // 再写两条，读必须看到全部已确认写入
        WriteResult w2 = c.write("n1", "r2", "v2");
        WriteResult w3 = c.write("n1", "r3", "v3");
        assertTrue(w2.accepted() && w3.accepted());
        ReadResult r2 = c.read("n1");
        logRead("n1", r2, "写入 r2,r3 确认后立即读");
        assertEquals(List.of("v1", "v2", "v3"),
                r2.entries().stream().map(LogEntry::payload).toList());
        assertEquals(w3.index(), r2.readIndex());

        // 读不改日志：条目数、任期、提交索引均不因读而变化
        int logSizeBefore = c.node("n1").getLog().size();
        long commitBefore = c.node("n1").getCommitIndex();
        for (int i = 0; i < 5; i++) {
            assertTrue(c.read("n1").success());
        }
        assertEquals(logSizeBefore, c.node("n1").getLog().size(), "读不得新增日志条目");
        assertEquals(commitBefore, c.node("n1").getCommitIndex(), "读不得推进提交索引");
        assertEquals(term, c.node("n1").getCurrentTerm(), "读不得改变任期");
        log.info("[assert] 连续 5 次读：logSize={} commitIndex={} term={} 均不变，依据=读只取快照不改状态",
                logSizeBefore, commitBefore, term);
    }

    // ----------------------------------------------------------------
    // 旧领导者被隔离后的读
    // ----------------------------------------------------------------

    @Test
    void isolatedOldLeaderCannotServeStaleReads() {
        Cluster c = newCluster();
        assertTrue(c.elect("n1"));
        assertTrue(c.write("n1", "r1", "v1").accepted());
        describe("n1 当选 term={} 并提交 v1", c.node("n1").getCurrentTerm());

        // 隔离旧领导者：它仍自以为是 LEADER，但读必须失败而不是返回本地旧数据
        c.isolate("n1");
        ReadResult stale = c.read("n1");
        logRead("n1", stale, "旧领导者被隔离在少数派");
        assertFalse(stale.success());
        assertEquals(RejectionReason.NO_QUORUM, stale.reason());
        assertTrue(stale.entries().isEmpty(), "失败的读不得回退为本地数据");
        assertEquals(-1, stale.readIndex());
        log.info("[assert] 隔离旧领导者读失败 reason={}，依据=心跳确认够不到多数派，拒绝返回本地旧数据", stale.reason());

        // 多数派侧选出新领导者并写入新数据
        assertTrue(c.elect("n2"));
        assertTrue(c.write("n2", "r2", "v2").accepted());
        describe("n2 当选 term={} 并提交 v2", c.node("n2").getCurrentTerm());

        // 旧领导者依然失败（本地只有 v1，绝不能把它当最新结果）
        ReadResult stillStale = c.read("n1");
        logRead("n1", stillStale, "新领导者已提交 v2 后，旧领导者仍被隔离");
        assertFalse(stillStale.success());
        assertEquals(RejectionReason.NO_QUORUM, stillStale.reason());

        // 新领导者的读是最新一致结果：包含隔离前已确认的 v1 与新写入的 v2
        ReadResult fresh = c.read("n2");
        logRead("n2", fresh, "新领导者读");
        assertTrue(fresh.success());
        assertEquals(c.node("n2").getCurrentTerm(), fresh.term());
        assertEquals(List.of("v1", "v2"), fresh.entries().stream().map(LogEntry::payload).toList());
        assertEquals(2, fresh.readIndex());
        log.info("[assert] 新领导者读 term={} readIndex={} 含全部已确认写入；旧领导者读持续失败，未泄露旧数据",
                fresh.term(), fresh.readIndex());

        // 分区恢复后旧领导者退位，读请求按 NOT_LEADER 失败并附重定向提示
        c.heal("n1");
        c.replicate("n2");
        ReadResult afterHeal = c.read("n1");
        logRead("n1", afterHeal, "分区恢复后旧领导者已退位");
        assertFalse(afterHeal.success());
        assertEquals(RejectionReason.NOT_LEADER, afterHeal.reason());
        assertEquals("n2", afterHeal.leaderId(), "应附当前领导者提示供客户端重定向");
        log.info("[assert] 退位节点读失败 reason={} leaderHint={}，依据=非领导者不受理一致性读",
                afterHeal.reason(), afterHeal.leaderId());
    }

    // ----------------------------------------------------------------
    // 够不到多数派 / 确认超时：失败原因可区分
    // ----------------------------------------------------------------

    @Test
    void readFailureReasonsAreDistinctAndRetryHasNoSideEffects() {
        Cluster c = newCluster();
        // 集群刚启动、尚无领导者：读直接失败，原因是 NOT_LEADER
        ReadResult noLeader = c.read("n1");
        logRead("n1", noLeader, "集群未选举");
        assertFalse(noLeader.success());
        assertEquals(RejectionReason.NOT_LEADER, noLeader.reason());
        assertNull(noLeader.leaderId(), "尚无已知领导者");

        assertTrue(c.elect("n1"));
        assertTrue(c.write("n1", "r1", "v1").accepted());
        int logSize = c.node("n1").getLog().size();

        // 确认超时：给两个副本注入 300ms 响应延迟，读确认时限 100ms
        c.setReadConfirmTimeoutMillis(100);
        c.setLatency("n2", 300);
        c.setLatency("n3", 300);
        ReadResult timeout = c.read("n1");
        logRead("n1", timeout, "副本响应延迟 300ms > 确认时限 100ms");
        assertFalse(timeout.success());
        assertEquals(RejectionReason.READ_CONFIRM_TIMEOUT, timeout.reason());
        assertTrue(timeout.entries().isEmpty());
        log.info("[assert] 确认超时 reason={}，依据=多数派确认未在 100ms 时限内完成", timeout.reason());

        // 够不到多数派：与超时不同——副本直接不可达
        c.clearLatency("n2");
        c.clearLatency("n3");
        c.isolate("n1");
        ReadResult noQuorum = c.read("n1");
        logRead("n1", noQuorum, "领导者被隔离");
        assertFalse(noQuorum.success());
        assertEquals(RejectionReason.NO_QUORUM, noQuorum.reason());
        assertNotEquals(timeout.reason(), noQuorum.reason(), "超时与不可达必须是可区分的失败原因");
        log.info("[assert] 不可达 reason={} 与超时 reason={} 可区分", noQuorum.reason(), timeout.reason());

        // 失败重试无副作用：日志、任期、提交索引均未因失败读而改变
        assertEquals(logSize, c.node("n1").getLog().size(), "失败的读不得新增日志条目");
        assertEquals(1, c.node("n1").getCommitIndex(), "失败的读不得改变提交索引");
        log.info("[assert] 两次失败读后 logSize={} commitIndex={} 不变，依据=读失败无副作用", logSize, 1);

        // 恢复后重试成功，内容完整
        c.heal("n1");
        ReadResult retry = c.read("n1");
        logRead("n1", retry, "分区恢复后重试读");
        assertTrue(retry.success());
        assertEquals(List.of("v1"), retry.entries().stream().map(LogEntry::payload).toList());
        assertEquals(1, retry.readIndex());
    }

    // ----------------------------------------------------------------
    // 选举进行中 / 领导者未确认的窗口
    // ----------------------------------------------------------------

    @Test
    void readDuringElectionAndUnconfirmedLeaderWindow() {
        Cluster c = newCluster();

        // 选举进行中：候选人自己也不能提供一致性读
        c.isolate("n1");
        assertFalse(c.elect("n1"), "被隔离的候选人收不到票");
        assertEquals(NodeRole.CANDIDATE, c.node("n1").getRole());
        ReadResult duringElection = c.read("n1");
        logRead("n1", duringElection, "选举进行中（候选人在少数派）");
        assertFalse(duringElection.success());
        assertEquals(RejectionReason.NOT_LEADER, duringElection.reason());
        log.info("[assert] 选举窗口读失败 reason={}，依据=候选人不是已确认领导者", duringElection.reason());

        // 多数派侧选出新领导者，但尚未提交本任期条目：读以 NO_LEADER 失败
        assertTrue(c.elect("n2"));
        ReadResult unconfirmed = c.read("n2");
        logRead("n2", unconfirmed, "新领导者尚未提交本任期条目");
        assertFalse(unconfirmed.success());
        assertEquals(RejectionReason.NO_LEADER, unconfirmed.reason());
        log.info("[assert] 未确认领导者读失败 reason={} term={}，依据=本任期无已提交条目，提交索引可能落后",
                unconfirmed.reason(), c.node("n2").getCurrentTerm());

        // 新领导者提交本任期条目后，读立即可用且位置正确
        assertTrue(c.write("n2", "r1", "v1").accepted());
        ReadResult ok = c.read("n2");
        logRead("n2", ok, "领导者提交本任期条目后");
        assertTrue(ok.success());
        assertEquals(c.node("n2").getCurrentTerm(), ok.term());
        assertEquals(1, ok.readIndex());
        log.info("[assert] 领导者确认后读成功 term={} readIndex={}", ok.term(), ok.readIndex());
    }

    // ----------------------------------------------------------------
    // 并发读写
    // ----------------------------------------------------------------

    @Test
    void concurrentReadsAndWritesStayLinearizable() throws InterruptedException {
        Cluster c = newCluster();
        assertTrue(c.elect("n1"));
        assertTrue(c.write("n1", "seed", "v0").accepted());
        int writes = 20;
        int readers = 4;
        ExecutorService pool = Executors.newFixedThreadPool(readers + 1);
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger writesDone = new AtomicInteger();
        ConcurrentLinkedQueue<String> anomalies = new ConcurrentLinkedQueue<>();
        ConcurrentLinkedQueue<ReadResult> failedReads = new ConcurrentLinkedQueue<>();

        // 写线程：顺序写入 w0..w19
        pool.submit(() -> {
            try {
                start.await();
                for (int i = 0; i < writes; i++) {
                    WriteResult r = c.write("n1", "w" + i, "v" + (i + 1));
                    if (!r.accepted()) {
                        anomalies.add("写入 w" + i + " 被拒绝: " + r.reason());
                        return;
                    }
                    writesDone.incrementAndGet();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        // 读线程：持续读，校验每次成功的读都是连续前缀且同一读者位置不回退
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
                        // 读到的必须是 [1, readIndex] 的连续前缀：无空洞、无重复、无错位
                        for (int i = 0; i < entries.size(); i++) {
                            if (entries.get(i).index() != i + 1) {
                                anomalies.add("读者" + rid + " 读到不连续序列: pos=" + (i + 1)
                                        + " entry=" + entries.get(i).index());
                            }
                        }
                        if (entries.size() != r.readIndex()) {
                            anomalies.add("读者" + rid + " entries 数=" + entries.size()
                                    + " 与 readIndex=" + r.readIndex() + " 不符");
                        }
                        if (r.readIndex() < lastReadIndex) {
                            anomalies.add("读者" + rid + " 读位置回退: " + lastReadIndex + " -> " + r.readIndex());
                        }
                        lastReadIndex = r.readIndex();
                    }
                    log.info("[read] 读者{} 完成，最终读位置 readIndex={}", rid, lastReadIndex);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(60, TimeUnit.SECONDS));

        assertTrue(anomalies.isEmpty(), () -> "并发读写出现一致性问题: " + new ArrayList<>(anomalies));
        assertTrue(failedReads.isEmpty(),
                () -> "无分区场景下读不应失败: " + failedReads.stream().map(ReadResult::reason).toList());
        log.info("[assert] 并发期间全部读成功且为连续前缀、读位置单调，依据=读以多数派确认后的 commitIndex 为上界");

        // 全部写完成后的读必须看到全部已确认写入
        ReadResult finalRead = c.read("n1");
        logRead("n1", finalRead, "全部写入完成后");
        assertTrue(finalRead.success());
        assertEquals(writes + 1, finalRead.readIndex(), "应读到 seed + 全部并发写入");
        assertEquals("v" + writes, finalRead.entries().get(finalRead.entries().size() - 1).payload());

        // 读不产生日志：领导者日志条目数 == seed + 写入数，任期未被读改变
        assertEquals(writes + 1, c.node("n1").getLog().size(), "读不得产生额外日志条目");
        assertEquals(1, c.node("n1").getCurrentTerm(), "读不得改变任期");
        log.info("[assert] 最终 readIndex={} term={}，日志条目数={} 与写入数一致，读无副作用",
                finalRead.readIndex(), c.node("n1").getCurrentTerm(), c.node("n1").getLog().size());
    }
}
