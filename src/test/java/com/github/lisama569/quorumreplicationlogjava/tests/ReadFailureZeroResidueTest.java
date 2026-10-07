package com.github.lisama569.quorumreplicationlogjava.tests;

import com.github.lisama569.quorumreplicationlogjava.replication.Cluster;
import com.github.lisama569.quorumreplicationlogjava.replication.LogEntry;
import com.github.lisama569.quorumreplicationlogjava.replication.RaftNode;
import com.github.lisama569.quorumreplicationlogjava.replication.ReadResult;
import com.github.lisama569.quorumreplicationlogjava.replication.RejectionReason;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 读失败分类与“失败 + 重试零残留”验证。
 *
 * <p>三类失败彼此可区分：{@code NO_QUORUM}（够不到多数派）、
 * {@code READ_CONFIRM_TIMEOUT}（确认迟迟不回）、{@code NO_LEADER}/
 * {@code NOT_LEADER}（没有可确认领导者）。每类失败前后都对全部节点
 * 做 term/role/commitIndex/日志（内容+条数）快照比对。
 */
class ReadFailureZeroResidueTest {

    private static final Logger log = LoggerFactory.getLogger(ReadFailureZeroResidueTest.class);
    private static final List<String> IDS = List.of("n1", "n2", "n3");

    private record NodeState(long term, String role, long commitIndex, List<LogEntry> log) {
        static NodeState of(RaftNode n) {
            return new NodeState(n.getCurrentTerm(), n.getRole().name(), n.getCommitIndex(), n.getLog());
        }
    }

    private Cluster newCluster() {
        return new Cluster(IDS);
    }

    private Map<String, NodeState> snapshot(Cluster c, String why) {
        Map<String, NodeState> snap = new LinkedHashMap<>();
        for (String id : c.nodeIds()) {
            NodeState s = NodeState.of(c.node(id));
            snap.put(id, s);
            log.info("[state] node={} role={} term={} commitIndex={} logSize={} log={} | {}",
                    id, s.role(), s.term(), s.commitIndex(), s.log().size(),
                    s.log().stream().map(e -> e.index() + "@t" + e.term() + ":" + e.payload()).toList(), why);
        }
        return snap;
    }

    private void assertStatesUnchanged(Map<String, NodeState> before, Cluster c, String why) {
        for (String id : before.keySet()) {
            NodeState after = NodeState.of(c.node(id));
            assertEquals(before.get(id), after, () -> "失败的读改动了节点 " + id + ": " + why);
        }
        log.info("[assert] 失败读/重试前后全部节点状态（term/role/commitIndex/日志内容与条数）完全一致 | {}", why);
    }

    private void logRead(String who, ReadResult r, String why) {
        if (r.success()) {
            log.info("[read] node={} 成功 term={} readIndex={} entries={} | {}",
                    who, r.term(), r.readIndex(),
                    r.entries().stream().map(e -> e.index() + "@t" + e.term() + ":" + e.payload()).toList(), why);
        } else {
            log.info("[read] node={} 失败 reason={} leaderHint={} | {}",
                    who, r.reason(), r.leaderId(), why);
        }
    }

    // ----------------------------------------------------------------
    // NO_QUORUM：领导者被隔离，失败 + 多次重试零残留（含落后副本恢复场景）
    // ----------------------------------------------------------------

    @Test
    void noQuorumReadFailuresAndRetriesLeaveNoResidue() {
        Cluster c = newCluster();
        assertTrue(c.elect("n1"));
        assertTrue(c.write("n1", "r1", "v1").accepted());
        c.replicate("n1"); // 基线：所有跟随者 commitIndex 先到 1

        // 再让 n3 掉队：隔离 n3 后提交第二条
        c.isolate("n3");
        assertTrue(c.write("n1", "r2", "v2").accepted());
        c.replicate("n1"); // n2 收到 index=2；n3 仍停在 index=1/commitIndex=1
        log.info("[case] n3 掉队停在 index=1/commitIndex=1；n1,n2 已到 index=2");

        // 同时隔离领导者与落后副本：n1 读够不到多数派
        c.isolate("n1"); // n1↔n2、n1↔n3 全断
        Map<String, NodeState> before = snapshot(c, "失败读之前（n1、n3 均被隔离）");

        for (int i = 0; i < 3; i++) {
            ReadResult r = c.read("n1");
            logRead("n1", r, "隔离领导者第 " + (i + 1) + " 次读");
            assertFalse(r.success());
            assertEquals(RejectionReason.NO_QUORUM, r.reason());
            assertTrue(r.entries().isEmpty());
            assertEquals(-1, r.readIndex());
        }
        assertStatesUnchanged(before, c, "隔离领导者 3 次失败读");

        // 落后副本 n3 恢复与“集群其余部分”的连通（但 n1 仍被隔离，读仍必失败）。
        // 旧实现里读确认会捎带日志：即便读失败，只要消息能到达 n3 就会补日志/提提交位置；
        // 正确实现下失败读的确认消息根本发不到 n3，n3 状态必须纹丝不动。
        c.connect("n2", "n3");
        for (int i = 0; i < 2; i++) {
            ReadResult r = c.read("n1");
            logRead("n1", r, "落后副本对集群恢复（n1 仍隔离）后第 " + (i + 1) + " 次重试");
            assertFalse(r.success());
            assertEquals(RejectionReason.NO_QUORUM, r.reason());
        }
        // n3 此刻仍停留在掉队时的位置（失败读没有借任何路径碰到它）
        assertEquals(1, c.node("n3").getLog().size(), "失败读不得给恢复连通的落后副本补日志");
        assertEquals(1, c.node("n3").getCommitIndex(), "失败读不得推进落后副本提交位置");
        // n1/n2 与隔离时的快照逐字节一致（失败读对任何节点零残留）
        for (String id : List.of("n1", "n2")) {
            assertEquals(before.get(id), NodeState.of(c.node(id)),
                    () -> "失败的读改动了节点 " + id);
        }
        log.info("[assert] 落后副本 n3 恢复连通后多次失败读，日志仍为 index=1、commitIndex=1；n1/n2 状态零残留");

        // 对照组：n1 恢复连通后读成功，且成功读本身仍零副作用（n3 由真正复制补齐）
        c.heal("n1");
        Map<String, NodeState> healedBefore = snapshot(c, "n1 恢复连通后、读之前");
        ReadResult ok = c.read("n1");
        logRead("n1", ok, "n1 恢复连通后的读");
        assertTrue(ok.success());
        assertEquals(2, ok.readIndex());
        assertStatesUnchanged(healedBefore, c, "成功读同样不复制日志（n3 仍停在 index=1，待 replicate 追赶）");
        assertEquals(1, c.node("n3").getLog().size(), "成功读也不得捎带日志给 n3");
        c.replicate("n1");
        c.replicate("n1");
        assertEquals(2, c.node("n3").getCommitIndex(), "只有显式复制才推进 n3");
        log.info("[assert] 对照组：replicate() 之后 n3 才补齐 commitIndex=2");
    }

    // ----------------------------------------------------------------
    // READ_CONFIRM_TIMEOUT：确认迟迟不回，失败与重试零残留
    // ----------------------------------------------------------------

    @Test
    void timeoutReadFailuresAndRetriesLeaveNoResidue() {
        Cluster c = newCluster();
        assertTrue(c.elect("n1"));
        assertTrue(c.write("n1", "r1", "v1").accepted());
        c.setReadConfirmTimeoutMillis(50);
        c.setLatency("n2", 300);
        c.setLatency("n3", 300);
        log.info("[case] 读确认时限 50ms，n2/n3 响应延迟 300ms");

        Map<String, NodeState> before = snapshot(c, "超时读之前");
        for (int i = 0; i < 2; i++) {
            ReadResult r = c.read("n1");
            logRead("n1", r, "副本慢响应第 " + (i + 1) + " 次读");
            assertFalse(r.success());
            assertEquals(RejectionReason.READ_CONFIRM_TIMEOUT, r.reason(),
                    "多数派确认在时限后才凑齐应判超时");
            assertTrue(r.entries().isEmpty());
            assertEquals(-1, r.readIndex());
        }
        assertStatesUnchanged(before, c, "两次确认超时读");

        // 清除延迟后重试成功，且成功前后各节点状态仍无读副作用
        c.clearLatency("n2");
        c.clearLatency("n3");
        ReadResult ok = c.read("n1");
        logRead("n1", ok, "延迟清除后重试");
        assertTrue(ok.success());
        assertEquals(1, ok.readIndex());
        assertStatesUnchanged(before, c, "超时恢复后成功读（快照仍一致：成功读同样不改状态）");
    }

    // ----------------------------------------------------------------
    // NO_LEADER / NOT_LEADER：没有可确认领导者时直接失败，绝不回退本地旧数据
    // ----------------------------------------------------------------

    @Test
    void noLeaderWindowFailuresLeaveNoResidue() {
        Cluster c = newCluster();
        // 无任何选举：NOT_LEADER
        Map<String, NodeState> empty = snapshot(c, "集群初始（无领导者）");
        ReadResult nobody = c.read("n1");
        logRead("n1", nobody, "集群未选举");
        assertFalse(nobody.success());
        assertEquals(RejectionReason.NOT_LEADER, nobody.reason());
        assertStatesUnchanged(empty, c, "无领导者读失败");

        // 新当选但本任期尚无已提交条目：NO_LEADER
        assertTrue(c.elect("n1"));
        assertTrue(c.write("n1", "r1", "v1").accepted());
        assertTrue(c.elect("n2"), "n2 凭完整日志在更高任期当选");
        long term2 = c.node("n2").getCurrentTerm();
        Map<String, NodeState> before = snapshot(c, "n2 当选 term=" + term2 + " 但未提交本任期条目");

        ReadResult unconfirmed = c.read("n2");
        logRead("n2", unconfirmed, "新领导者本任期未提交任何条目");
        assertFalse(unconfirmed.success());
        assertEquals(RejectionReason.NO_LEADER, unconfirmed.reason());
        assertEquals(-1, unconfirmed.readIndex());
        assertTrue(unconfirmed.entries().isEmpty(), "不得把本地旧日志当成功结果返回");

        ReadResult retry = c.read("n2");
        assertFalse(retry.success());
        assertEquals(RejectionReason.NO_LEADER, retry.reason(), "重试仍是可区分的同一失败原因");
        assertStatesUnchanged(before, c, "未确认领导者窗口两次失败读");
        log.info("[assert] term={} 未确认窗口读一律 NO_LEADER，不返回 n2 本地已有的 v1 旧数据", term2);
    }

    // ----------------------------------------------------------------
    // 选举进行中的窗口：候选人/旧领导者读失败且零副作用，恢复后读到最新值
    // ----------------------------------------------------------------

    @Test
    void readsDuringElectionWindowFailCleanlyAndNeverServeOldData() {
        Cluster c = newCluster();
        assertTrue(c.elect("n1"));
        assertTrue(c.write("n1", "r1", "v1").accepted());
        c.replicate("n1");
        log.info("[case] n1 term=1 提交 v1(index=1) 并同步各跟随者");

        // 旧领导者被隔离，多数派侧进入更高任期选举：n2 成为 term=2 候选人
        c.isolate("n1");
        c.isolate("n3"); // n3 暂时也与 n2 断开 -> n2 选举无法完成，停在候选人
        boolean elected = c.elect("n2");
        assertFalse(elected, "n2 只拿到自己一票，选举悬而未决");
        long candidateTerm = c.node("n2").getCurrentTerm();

        Map<String, NodeState> before = snapshot(c,
                "选举窗口：n2 为 term=" + candidateTerm + " 候选人，n1 是被隔离的旧领导者");

        // 候选人读：NOT_LEADER
        ReadResult candidateRead = c.read("n2");
        logRead("n2", candidateRead, "选举进行中（候选人）");
        assertFalse(candidateRead.success());
        assertEquals(RejectionReason.NOT_LEADER, candidateRead.reason());

        // 被隔离旧领导者读：NO_QUORUM（多数派确认失败），绝不返回本地旧数据冒充最新
        ReadResult staleRead = c.read("n1");
        logRead("n1", staleRead, "旧领导者在更高任期已开始的选举期间");
        assertFalse(staleRead.success());
        assertEquals(RejectionReason.NO_QUORUM, staleRead.reason());
        assertTrue(staleRead.entries().isEmpty());
        assertEquals(-1, staleRead.readIndex());

        assertStatesUnchanged(before, c, "选举窗口内候选人/旧领导者各一次失败读");

        // 恢复 n3 完成选举，新领导者本任期还没提交条目：NO_LEADER
        c.connect("n2", "n3");
        assertTrue(c.elect("n2"));
        long term2 = c.node("n2").getCurrentTerm();
        ReadResult unconfirmed = c.read("n2");
        logRead("n2", unconfirmed, "term=" + term2 + " 刚当选，本任期未提交");
        assertFalse(unconfirmed.success());
        assertEquals(RejectionReason.NO_LEADER, unconfirmed.reason());

        // 本任期提交一条后读立即可用，且必须包含旧任期已确认的 v1（不返回更旧值）
        assertTrue(c.write("n2", "r2", "v2").accepted());
        ReadResult fresh = c.read("n2");
        logRead("n2", fresh, "term=" + term2 + " 提交 v2 后");
        assertTrue(fresh.success());
        assertEquals(term2, fresh.term());
        assertEquals(2, fresh.readIndex());
        assertEquals(List.of("v1", "v2"),
                fresh.entries().stream().map(LogEntry::payload).toList());
        log.info("[assert] 选举窗口读全部失败且零副作用；新任期提交后读到连续前缀 v1,v2（不漏旧确认值）");
    }
}
