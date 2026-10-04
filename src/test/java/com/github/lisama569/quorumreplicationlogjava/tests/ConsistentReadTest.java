package com.github.lisama569.quorumreplicationlogjava.tests;

import com.github.lisama569.quorumreplicationlogjava.replication.Cluster;
import com.github.lisama569.quorumreplicationlogjava.replication.LogEntry;
import com.github.lisama569.quorumreplicationlogjava.replication.NodeRole;
import com.github.lisama569.quorumreplicationlogjava.replication.ReadFailureReason;
import com.github.lisama569.quorumreplicationlogjava.replication.ReadResult;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
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
 * 一致性读（ReadIndex 风格）的行为验证：写后立即可读、旧领导者隔离后不得返回旧数据、
 * 失败原因可区分（无领导者/不够多数派/确认超时）、并发读写、选举窗口中的读、
 * 重启领导者经多数派确认恢复提交位置。日志中打印任期、读到的日志位置与判定依据。
 */
class ConsistentReadTest {

    private static final Logger log = LoggerFactory.getLogger(ConsistentReadTest.class);
    private static final List<String> IDS = List.of("n1", "n2", "n3");
    private static final long READ_TIMEOUT_MS = 2_000;

    private Cluster newCluster() {
        return new Cluster(IDS);
    }

    private void describe(String fmt, Object... args) {
        log.info("[case] " + fmt, args);
    }

    private void logRead(String tag, ReadResult r) {
        if (r.success()) {
            log.info("[read] {} 成功: term={} readIndex={} entries={} leader={}", tag,
                    r.term(), r.readIndex(),
                    r.entries().stream().map(e -> e.index() + "@t" + e.term() + ":" + e.payload()).toList(),
                    r.leaderId());
        } else {
            log.info("[read] {} 失败: reason={} leaderHint={}", tag, r.reason(), r.leaderId());
        }
    }

    // ----------------------------------------------------------------
    // 写完之后马上读
    // ----------------------------------------------------------------

    @Test
    void readAfterCommittedWriteSeesItWithTermAndPosition() {
        Cluster c = newCluster();
        assertTrue(c.elect("n1"));
        long term = c.node("n1").getCurrentTerm();
        assertTrue(c.write("n1", "r1", "v1").accepted());
        assertTrue(c.write("n1", "r2", "v2").accepted());
        describe("term={} 写入 r1,r2 已多数派确认，commitIndex={}", term, c.node("n1").getCommitIndex());

        // 写完马上读：必须看到已确认的写入，并给出读到的任期与日志位置
        ReadResult r = c.read("n1", READ_TIMEOUT_MS);
        logRead("写完即读@n1", r);
        assertTrue(r.success());
        assertEquals(term, r.term(), "读结果应带发生时的领导者任期");
        assertEquals(2, r.readIndex(), "读位置应覆盖全部已确认写入");
        assertEquals("n1", r.leaderId());
        assertEquals(List.of("v1", "v2"), r.entries().stream().map(LogEntry::payload).toList());
        log.info("[assert] 判定依据=读前多数派确认领导者有效，返回截至 commitIndex={} 的已提交条目 term={}",
                r.readIndex(), r.term());

        // 跟随者不直接服务读：给出 NOT_LEADER 与领导者提示，而非本地数据
        ReadResult follower = c.read("n2", READ_TIMEOUT_MS);
        logRead("跟随者读@n2", follower);
        assertFalse(follower.success());
        assertEquals(ReadFailureReason.NOT_LEADER, follower.reason());
        assertEquals("n1", follower.leaderId(), "应给出已知领导者提示便于重定向");
        assertTrue(follower.entries().isEmpty(), "失败的读不得夹带本地数据");
        log.info("[assert] 跟随者读被拒绝 reason={} leaderHint={}，依据=仅经多数派确认的领导者可服务读",
                follower.reason(), follower.leaderId());
    }

    // ----------------------------------------------------------------
    // 旧领导者被隔离后的读
    // ----------------------------------------------------------------

    @Test
    void isolatedOldLeaderCannotServeStaleReads() {
        Cluster c = newCluster();
        assertTrue(c.elect("n1"));
        assertTrue(c.write("n1", "r1", "v1").accepted());
        describe("term=1 写入 v1 已提交，commitIndex={}", c.node("n1").getCommitIndex());

        // 隔离旧领导者，多数派侧选出新领导者并继续写入
        c.isolate("n1");
        assertTrue(c.elect("n2"));
        assertTrue(c.write("n2", "r2", "v2").accepted());
        long newTerm = c.node("n2").getCurrentTerm();
        describe("n1 被隔离；n2 当选 term={} 并提交 v2，commitIndex={}", newTerm, c.node("n2").getCommitIndex());

        // 旧领导者自以为还是领导者，但无法通过多数派确认：读必须失败而非返回本地旧数据
        assertEquals(NodeRole.LEADER, c.node("n1").getRole(), "旧领导者未察觉自己已被取代");
        ReadResult stale = c.read("n1", READ_TIMEOUT_MS);
        logRead("隔离旧领导者读@n1", stale);
        assertFalse(stale.success(), "少数派中的旧领导者不得把本地旧数据当成最新结果返回");
        assertEquals(ReadFailureReason.NO_QUORUM, stale.reason());
        assertTrue(stale.entries().isEmpty());
        log.info("[assert] 旧领导者读失败 reason={}，依据=确认轮未获多数派回应（本地虽有 v1 也不返回）",
                stale.reason());

        // 新领导者的读是最新一致结果：同时看到 v1（换主前已确认）与 v2
        ReadResult fresh = c.read("n2", READ_TIMEOUT_MS);
        logRead("新领导者读@n2", fresh);
        assertTrue(fresh.success());
        assertEquals(newTerm, fresh.term());
        assertEquals(2, fresh.readIndex());
        assertEquals(List.of("v1", "v2"), fresh.entries().stream().map(LogEntry::payload).toList());
        log.info("[assert] 新领导者读到 term={} readIndex={} 含换主前已确认的 v1，依据=领导者完整性+多数派确认",
                fresh.term(), fresh.readIndex());
    }

    // ----------------------------------------------------------------
    // 失败原因可区分：无领导者 / 不够多数派 / 确认超时
    // ----------------------------------------------------------------

    @Test
    void readFailuresAreDistinctAndNeverFallBackToLocalData() {
        // (1) 集群尚未选出领导者：NO_LEADER
        Cluster c = newCluster();
        ReadResult noLeader = c.read("n1", READ_TIMEOUT_MS);
        logRead("未选举读@n1", noLeader);
        assertFalse(noLeader.success());
        assertEquals(ReadFailureReason.NO_LEADER, noLeader.reason());
        assertNull(noLeader.leaderId());
        log.info("[assert] 无领导者读失败 reason={}，依据=本节点非领导者且无已知领导者", noLeader.reason());

        // (2) 领导者够不到多数派：NO_QUORUM
        assertTrue(c.elect("n1"));
        assertTrue(c.write("n1", "r1", "v1").accepted());
        c.isolate("n1");
        ReadResult noQuorum = c.read("n1", READ_TIMEOUT_MS);
        logRead("少数派领导者读@n1", noQuorum);
        assertFalse(noQuorum.success());
        assertEquals(ReadFailureReason.NO_QUORUM, noQuorum.reason());
        log.info("[assert] 多数派不可达读失败 reason={}，依据=确认轮完成但确认数不足多数派", noQuorum.reason());

        // (3) 确认消息迟迟无回应：CONFIRM_TIMEOUT（与 NO_QUORUM 可区分）
        c.heal("n1");
        c.setReadConfirmDelayMillis(500); // 确认消息被人为延迟
        ReadResult timeout = c.read("n1", 100);
        logRead("确认超时读@n1", timeout);
        assertFalse(timeout.success());
        assertEquals(ReadFailureReason.CONFIRM_TIMEOUT, timeout.reason());
        log.info("[assert] 确认超时读失败 reason={}，依据=100ms 内未集齐多数派确认（消息延迟 500ms）",
                timeout.reason());
        assertNotEquals(noQuorum.reason(), timeout.reason(), "超时与不够多数派必须可区分");

        // 延迟消除后同一节点读恢复成功，且失败重试无副作用
        c.setReadConfirmDelayMillis(0);
        ReadResult recovered = c.read("n1", READ_TIMEOUT_MS);
        logRead("延迟消除后读@n1", recovered);
        assertTrue(recovered.success());
        assertEquals(1, recovered.readIndex());
        assertEquals(1, c.node("n1").getLog().size(), "读（含失败重试）不得向日志追加条目");
        log.info("[assert] 恢复后读成功 term={} readIndex={}，日志条目数不变，依据=读不改日志、重试无副作用",
                recovered.term(), recovered.readIndex());
    }

    // ----------------------------------------------------------------
    // 选举进行中的读
    // ----------------------------------------------------------------

    @Test
    void readDuringElectionWindowFailsCleanlyWithoutSideEffects() {
        Cluster c = newCluster();
        assertTrue(c.elect("n1"));
        assertTrue(c.write("n1", "r1", "v1").accepted());

        // n3 被隔离并发起选举：选不上，停留在候选人状态，选举窗口中的读应失败
        c.isolate("n3");
        assertFalse(c.elect("n3"), "被隔离的候选人无法集齐多数派选票");
        assertEquals(NodeRole.CANDIDATE, c.node("n3").getRole());
        int logSizeBefore = c.node("n3").getLog().size();
        ReadResult duringElection = c.read("n3", READ_TIMEOUT_MS);
        logRead("选举窗口读@n3", duringElection);
        assertFalse(duringElection.success());
        assertEquals(ReadFailureReason.NO_LEADER, duringElection.reason(),
                "候选人没有可确认的领导者，读应失败而非返回本地数据");
        assertTrue(duringElection.entries().isEmpty());
        log.info("[assert] 选举窗口读失败 reason={} term={}，依据=候选人非领导者且不再信任旧领导者",
                duringElection.reason(), c.node("n3").getCurrentTerm());

        // 失败的读不改日志、不产生条目，重试无副作用
        assertEquals(logSizeBefore, c.node("n3").getLog().size(), "读不得改变日志");
        ReadResult retry = c.read("n3", READ_TIMEOUT_MS);
        assertFalse(retry.success());
        assertEquals(logSizeBefore, c.node("n3").getLog().size(), "失败重试不应有副作用");

        // 选举窗口结束后：n3 的参选抬高了集群任期，旧领导者心跳被拒后退位，
        // 重新选举出新领导者，读恢复一致且内容不变
        c.heal("n3");
        c.replicate("n1"); // n1 发现更高任期后退位
        assertNotEquals(NodeRole.LEADER, c.node("n1").getRole());
        assertTrue(c.elect("n1"), "任期抬高后重新选举");
        c.replicate("n1");
        ReadResult after = c.read("n1", READ_TIMEOUT_MS);
        logRead("窗口结束后读@n1", after);
        assertTrue(after.success());
        assertEquals(List.of("v1"), after.entries().stream().map(LogEntry::payload).toList());
        ReadResult redirected = c.read("n3", READ_TIMEOUT_MS);
        assertEquals(ReadFailureReason.NOT_LEADER, redirected.reason());
        assertEquals("n1", redirected.leaderId(), "恢复后跟随者应重新给出领导者提示");
        log.info("[assert] 窗口结束后读一致 term={} readIndex={}；跟随者重定向到 {}",
                after.term(), after.readIndex(), redirected.leaderId());
    }

    // ----------------------------------------------------------------
    // 并发读写
    // ----------------------------------------------------------------

    @Test
    void concurrentReadsNeverSeeStaleOrTornLog() throws InterruptedException {
        Cluster c = newCluster();
        assertTrue(c.elect("n1"));
        int totalWrites = 30;
        int readers = 4;
        AtomicInteger acknowledged = new AtomicInteger();
        List<String> violations = new CopyOnWriteArrayList<>();
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(readers + 1);

        // 单写者顺序写入，已确认计数单调递增
        pool.submit(() -> {
            try {
                start.await();
                for (int i = 0; i < totalWrites; i++) {
                    var w = c.write("n1", "w-" + i, "v-" + i);
                    if (w.accepted()) {
                        acknowledged.incrementAndGet();
                    } else {
                        violations.add("写入 w-" + i + " 未确认: " + w.reason());
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        // 多读者并发读：成功的读不得落后于读开始前已确认的位置，日志不得有空洞/错位/重复
        for (int r = 0; r < readers; r++) {
            final int rid = r;
            pool.submit(() -> {
                try {
                    start.await();
                    long lastReadIndex = 0;
                    while (acknowledged.get() < totalWrites) {
                        int snapshot = acknowledged.get();
                        ReadResult res = c.read("n1", READ_TIMEOUT_MS);
                        if (!res.success()) {
                            violations.add("reader" + rid + " 读失败: " + res.reason());
                            continue;
                        }
                        if (res.readIndex() < snapshot) {
                            violations.add("reader" + rid + " 读到旧值: readIndex=" + res.readIndex()
                                    + " < 读前已确认=" + snapshot);
                        }
                        if (res.readIndex() < lastReadIndex) {
                            violations.add("reader" + rid + " 读位置倒退: " + res.readIndex()
                                    + " < " + lastReadIndex);
                        }
                        lastReadIndex = Math.max(lastReadIndex, res.readIndex());
                        List<LogEntry> entries = res.entries();
                        if (entries.size() != res.readIndex()) {
                            violations.add("reader" + rid + " 条目数与读位置不符: " + entries.size()
                                    + " != " + res.readIndex());
                        }
                        for (int i = 0; i < entries.size(); i++) {
                            if (entries.get(i).index() != i + 1) {
                                violations.add("reader" + rid + " 日志空洞/错位: entries[" + i + "].index="
                                        + entries.get(i).index());
                            }
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(60, TimeUnit.SECONDS));

        assertTrue(violations.isEmpty(), () -> "并发读写违反一致性: " + violations);
        assertEquals(totalWrites, acknowledged.get());
        ReadResult finalRead = c.read("n1", READ_TIMEOUT_MS);
        logRead("并发结束后读@n1", finalRead);
        assertTrue(finalRead.success());
        assertEquals(totalWrites, finalRead.readIndex());
        for (int i = 0; i < totalWrites; i++) {
            assertEquals("v-" + i, finalRead.entries().get(i).payload(), "最终读应包含全部已确认写入且顺序一致");
        }
        log.info("[assert] {} 个读者与写者并发：无旧值/倒退/空洞/重复，最终 term={} readIndex={}",
                readers, finalRead.term(), finalRead.readIndex());
    }

    // ----------------------------------------------------------------
    // 重启后的读：经多数派确认恢复提交位置
    // ----------------------------------------------------------------

    @Test
    void restartedLeaderReadRecoversCommittedPositionViaQuorum() {
        Cluster c = newCluster();
        assertTrue(c.elect("n1"));
        assertTrue(c.write("n1", "r1", "v1").accepted());
        assertTrue(c.write("n1", "r2", "v2").accepted());
        describe("写入 v1,v2 已提交，commitIndex={}", c.node("n1").getCommitIndex());

        // 领导者重启：提交索引（易失状态）归零，日志从磁盘恢复
        c.restart("n1");
        assertEquals(0, c.node("n1").getCommitIndex());
        assertEquals(2, c.node("n1").getLog().size());
        assertTrue(c.elect("n1"), "重启后凭完整日志重新当选");
        long term = c.node("n1").getCurrentTerm();
        describe("n1 重启后重新当选 term={}，本地 commitIndex={} 待恢复", term, c.node("n1").getCommitIndex());

        // 读前确认轮从多数派上报中恢复提交位置：重启前已确认的写入必须可见
        ReadResult r = c.read("n1", READ_TIMEOUT_MS);
        logRead("重启领导者读@n1", r);
        assertTrue(r.success());
        assertEquals(term, r.term());
        assertEquals(2, r.readIndex(), "重启前已确认的写入对新领导者读必须可见");
        assertEquals(List.of("v1", "v2"), r.entries().stream().map(LogEntry::payload).toList());
        log.info("[assert] 重启领导者读恢复 readIndex={} term={}，依据=确认轮采纳多数派上报的合法提交索引",
                r.readIndex(), r.term());
    }
}
