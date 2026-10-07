package com.github.lisama569.quorumreplicationlogjava.tests;

import com.github.lisama569.quorumreplicationlogjava.replication.Cluster;
import com.github.lisama569.quorumreplicationlogjava.replication.LogEntry;
import com.github.lisama569.quorumreplicationlogjava.replication.NodeRole;
import com.github.lisama569.quorumreplicationlogjava.replication.RaftNode;
import com.github.lisama569.quorumreplicationlogjava.replication.ReadResult;
import com.github.lisama569.quorumreplicationlogjava.replication.RejectionReason;
import com.github.lisama569.quorumreplicationlogjava.replication.WriteResult;
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
 * 本轮重点回归：一致性读的零副作用验证。
 *
 * <p>核心不变量：读（无论成功/失败/重试）不改变任何节点的日志内容、
 * 条目数量和已提交位置（commitIndex）；只有真正的写入与复制才能推进它们。
 * 每个用例在读前后对全部节点做 (role, term, commitIndex, log) 快照并逐字段比对，
 * 日志打印任期、日志位置与判定依据。
 */
class ReadSideEffectFreeTest {

    private static final Logger log = LoggerFactory.getLogger(ReadSideEffectFreeTest.class);
    private static final List<String> IDS = List.of("n1", "n2", "n3");

    /** 节点可观测状态快照：角色、任期、已提交位置、完整日志。 */
    private record NodeState(NodeRole role, long term, long commitIndex, List<LogEntry> log) {

        static NodeState of(RaftNode n) {
            return new NodeState(n.getRole(), n.getCurrentTerm(), n.getCommitIndex(), n.getLog());
        }

        @Override
        public String toString() {
            return "role=" + role + " term=" + term + " commitIndex=" + commitIndex
                    + " log=" + log.stream().map(e -> e.index() + "@t" + e.term() + ":" + e.payload()).toList();
        }
    }

    private Map<String, NodeState> snapshotAll(Cluster c, String why) {
        Map<String, NodeState> snap = new LinkedHashMap<>();
        for (String id : c.nodeIds()) {
            NodeState s = NodeState.of(c.node(id));
            snap.put(id, s);
            log.info("[snapshot] node={} {} | {}", id, s, why);
        }
        return snap;
    }

    private void assertUnchanged(Cluster c, Map<String, NodeState> before, String why) {
        for (String id : c.nodeIds()) {
            NodeState now = NodeState.of(c.node(id));
            assertEquals(before.get(id), now, () -> "节点 " + id + " 的状态被读改变: " + why
                    + " | before=" + before.get(id) + " after=" + now);
        }
        log.info("[assert] 全部节点状态与读前一致（日志内容/条目数/已提交位置均未变）| {}", why);
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
    // 本轮重点：落后副本恢复连通后再发读，读不得改动任何副本的日志与已提交位置
    // ----------------------------------------------------------------

    @Test
    void readAfterLaggingFollowerReconnectsDoesNotMutateAnyReplica() {
        Cluster c = new Cluster(IDS);
        assertTrue(c.elect("n1"));
        assertTrue(c.write("n1", "r1", "v1").accepted());
        c.replicate("n1"); // 再补一轮心跳，让跟随者拿到 leaderCommit=1
        long term = c.node("n1").getCurrentTerm();
        log.info("[case] n1 当选 term={}，提交 r1=v1（三副本均已提交）", term);

        // 隔离 n3，多数派 (n1,n2) 继续提交两条；n3 掉队
        c.isolate("n3");
        assertTrue(c.write("n1", "r2", "v2").accepted());
        assertTrue(c.write("n1", "r3", "v3").accepted());
        assertEquals(1, c.node("n3").getLog().size(), "n3 被隔离，日志停留在 1 条");
        assertEquals(1, c.node("n3").getCommitIndex(), "n3 已提交位置停留在 1");
        log.info("[case] n3 隔离期间提交 r2,r3；n3 落后：logSize=1 commitIndex=1，"
                + "多数派 logSize=3 commitIndex=3");

        // 恢复连通但不触发任何复制：此后只有一次读
        c.heal("n3");
        Map<String, NodeState> before = snapshotAll(c, "n3 恢复连通后、读之前");

        // 一次成功的读：必须看到全部已确认写入，但不得补齐 n3 的日志或推进其提交位置
        ReadResult r = c.read("n1");
        logRead("n1", r, "落后副本 n3 刚恢复连通后的读");
        assertTrue(r.success());
        assertEquals(term, r.term());
        assertEquals(3, r.readIndex());
        assertEquals(List.of("v1", "v2", "v3"), r.entries().stream().map(LogEntry::payload).toList());

        assertUnchanged(c, before, "成功的读之后（重点观察落后副本 n3）");
        assertEquals(1, c.node("n3").getLog().size(),
                "读不得给落后副本补日志：n3 日志仍应只有 1 条");
        assertEquals(1, c.node("n3").getCommitIndex(),
                "读不得推进落后副本的已提交位置：n3 commitIndex 仍应为 1");
        log.info("[assert] 落后副本 n3 logSize={} commitIndex={} 未被读改动，"
                        + "依据=读确认不携带日志条目与 leaderCommit",
                c.node("n3").getLog().size(), c.node("n3").getCommitIndex());

        // 再连续读多次（含一次发向跟随者的失败读）：状态依然不变
        for (int i = 0; i < 3; i++) {
            assertTrue(c.read("n1").success());
        }
        ReadResult onFollower = c.read("n3");
        logRead("n3", onFollower, "发向落后跟随者的读");
        assertFalse(onFollower.success());
        assertEquals(RejectionReason.NOT_LEADER, onFollower.reason());
        assertUnchanged(c, before, "连续成功读 + 跟随者失败读之后");

        // 对照：真正的复制才允许推进 n3——验证测试环境本身能收敛
        c.replicate("n1");
        c.replicate("n1");
        assertEquals(3, c.node("n3").getLog().size());
        assertEquals(3, c.node("n3").getCommitIndex());
        log.info("[assert] 显式复制后 n3 才补齐 logSize=3 commitIndex=3，"
                + "依据=日志与已提交位置只能由真正的复制推进");
    }

    // ----------------------------------------------------------------
    // 本轮重点：被客户端拒绝（NO_QUORUM）的写入不得因一次读出现在其他副本上
    // ----------------------------------------------------------------

    @Test
    void rejectedWriteDoesNotLeakToOtherReplicasViaRead() {
        Cluster c = new Cluster(IDS);
        assertTrue(c.elect("n1"));
        assertTrue(c.write("n1", "r1", "v1").accepted());
        c.replicate("n1"); // 再补一轮心跳，让跟随者拿到 leaderCommit=1

        // 隔离领导者后写入：客户端收到失败，条目仅作为未提交数据留在 n1 本地
        c.isolate("n1");
        WriteResult rejected = c.write("n1", "r2", "rejected-x");
        assertFalse(rejected.accepted());
        assertEquals(RejectionReason.NO_QUORUM, rejected.reason());
        assertEquals(2, c.node("n1").getLog().size(), "未确认条目暂存于旧领导者本地");
        for (String id : List.of("n2", "n3")) {
            assertEquals(1, c.node(id).getLog().size(), id + " 不应收到未确认条目");
        }
        log.info("[case] 被隔离领导者的写入 r2=rejected-x 客户端已收到 NO_QUORUM 失败，"
                + "条目仅在 n1 本地（index=2 未提交）");

        // 恢复连通：n1 仍是 term=1 的领导者，读可以成功——但读不得把 rejected-x 带给别人
        c.heal("n1");
        Map<String, NodeState> before = snapshotAll(c, "分区恢复后、读之前");

        ReadResult r = c.read("n1");
        logRead("n1", r, "携带未确认条目的领导者发起的读");
        assertTrue(r.success());
        assertEquals(1, r.readIndex(), "读位置只覆盖已提交条目");
        assertEquals(List.of("v1"), r.entries().stream().map(LogEntry::payload).toList(),
                "读结果不得包含未被确认的 rejected-x");

        assertUnchanged(c, before, "读之后（重点观察 n2/n3 是否被注入 rejected-x）");
        for (String id : List.of("n2", "n3")) {
            List<LogEntry> logNow = c.node(id).getLog();
            assertEquals(1, logNow.size(), id + " 日志条目数不得因读而增加");
            assertTrue(logNow.stream().noneMatch(e -> e.payload().equals("rejected-x")),
                    id + " 上不得出现被客户端拒绝的写入 rejected-x");
            assertEquals(1, c.node(id).getCommitIndex(), id + " 已提交位置不得因读而推进");
        }
        assertEquals(1, c.node("n1").getCommitIndex(), "领导者的已提交位置也不得因读而推进");
        log.info("[assert] n2/n3 日志仍只有已提交的 v1，rejected-x 未泄漏；"
                + "各节点 commitIndex=1 不变，依据=读确认不复制日志、不携带 leaderCommit");
    }

    // ----------------------------------------------------------------
    // 失败读（NOT_LEADER / NO_QUORUM / READ_CONFIRM_TIMEOUT）重试零副作用
    // ----------------------------------------------------------------

    @Test
    void failedReadRetriesLeaveAllNodesUntouched() {
        Cluster c = new Cluster(IDS);
        assertTrue(c.elect("n1"));
        assertTrue(c.write("n1", "r1", "v1").accepted());

        // NOT_LEADER：发向跟随者的读
        Map<String, NodeState> before = snapshotAll(c, "各类失败读之前");
        for (int i = 1; i <= 3; i++) {
            ReadResult r = c.read("n2");
            assertFalse(r.success());
            assertEquals(RejectionReason.NOT_LEADER, r.reason());
        }
        log.info("[read] node=n2 失败 reason={} x3 次重试 | 发向跟随者", RejectionReason.NOT_LEADER);
        assertUnchanged(c, before, "NOT_LEADER 失败读重试 3 次后");

        // READ_CONFIRM_TIMEOUT：副本响应 300ms > 确认时限 100ms
        c.setReadConfirmTimeoutMillis(100);
        c.setLatency("n2", 300);
        c.setLatency("n3", 300);
        for (int i = 1; i <= 3; i++) {
            ReadResult r = c.read("n1");
            assertFalse(r.success());
            assertEquals(RejectionReason.READ_CONFIRM_TIMEOUT, r.reason());
        }
        log.info("[read] node=n1 失败 reason={} x3 次重试 | 副本延迟 300ms > 时限 100ms",
                RejectionReason.READ_CONFIRM_TIMEOUT);
        c.clearLatency("n2");
        c.clearLatency("n3");
        assertUnchanged(c, before, "READ_CONFIRM_TIMEOUT 失败读重试 3 次后");

        // NO_QUORUM：领导者被隔离
        c.isolate("n1");
        for (int i = 1; i <= 3; i++) {
            ReadResult r = c.read("n1");
            assertFalse(r.success());
            assertEquals(RejectionReason.NO_QUORUM, r.reason());
        }
        log.info("[read] node=n1 失败 reason={} x3 次重试 | 领导者被隔离", RejectionReason.NO_QUORUM);
        assertUnchanged(c, before, "NO_QUORUM 失败读重试 3 次后");

        // 恢复后读立即可用，且内容仍是已确认的 v1——失败期间没有任何残留
        c.heal("n1");
        ReadResult retry = c.read("n1");
        logRead("n1", retry, "全部失败路径恢复后的重试读");
        assertTrue(retry.success());
        assertEquals(List.of("v1"), retry.entries().stream().map(LogEntry::payload).toList());
        assertEquals(1, retry.readIndex());
        log.info("[assert] 三类失败读各重试 3 次，全节点 (role,term,commitIndex,log) 快照逐字段一致；"
                + "恢复后读成功 term={} readIndex={}，依据=失败路径不触碰日志与已提交位置",
                retry.term(), retry.readIndex());
    }

    // ----------------------------------------------------------------
    // 领导者未确认窗口（NO_LEADER）的读同样零副作用
    // ----------------------------------------------------------------

    @Test
    void readOnUnconfirmedLeaderFailsWithoutTouchingAnything() {
        Cluster c = new Cluster(IDS);
        assertTrue(c.elect("n1"), "空日志集群中 n1 当选");
        // 新领导者尚未提交本任期条目：读必须以 NO_LEADER 失败，且不得顺手写一条
        // “空条目/占位条目”来确认自己（日志条目数必须保持 0）
        Map<String, NodeState> before = snapshotAll(c, "新当选未确认领导者读之前");

        for (int i = 1; i <= 3; i++) {
            ReadResult r = c.read("n1");
            assertFalse(r.success());
            assertEquals(RejectionReason.NO_LEADER, r.reason());
        }
        log.info("[read] node=n1 失败 reason={} x3 次重试 | 新领导者尚未提交本任期条目 term={}",
                RejectionReason.NO_LEADER, c.node("n1").getCurrentTerm());

        assertUnchanged(c, before, "NO_LEADER 失败读重试 3 次后");
        assertEquals(0, c.node("n1").getLog().size(), "读不得为确认领导权而追加任何条目");
        assertEquals(0, c.node("n1").getCommitIndex(), "读不得推进已提交位置");
        log.info("[assert] 未确认领导者读失败重试零副作用：logSize=0 commitIndex=0 不变，"
                + "依据=读失败不得伪造日志条目或提交位置");
    }
}
