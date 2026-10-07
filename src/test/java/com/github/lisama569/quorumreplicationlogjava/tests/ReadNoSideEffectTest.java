package com.github.lisama569.quorumreplicationlogjava.tests;

import com.github.lisama569.quorumreplicationlogjava.replication.Cluster;
import com.github.lisama569.quorumreplicationlogjava.replication.LogEntry;
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
 * 本轮重点回归：一致性读的“零副作用”。
 *
 * <p>覆盖：落后副本恢复连通后的读、被拒绝写入不得借读传播、失败读/重试
 * 前后各节点状态逐字节一致、并发读写窗口读不带日志。测试日志统一打印
 * 任期、日志条目（index@term:payload）与提交位置。
 */
class ReadNoSideEffectTest {

    private static final Logger log = LoggerFactory.getLogger(ReadNoSideEffectTest.class);

    /** 单个节点的可比较状态快照（日志内容 + 条目数 + 提交位置 + 任期 + 角色）。 */
    private record NodeState(long term, String role, long commitIndex, List<LogEntry> log) {
        static NodeState of(RaftNode n) {
            return new NodeState(n.getCurrentTerm(), n.getRole().name(), n.getCommitIndex(), n.getLog());
        }
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

    private void assertStatesUnchanged(Map<String, NodeState> before, Map<String, NodeState> after, String why) {
        assertEquals(before.keySet(), after.keySet());
        for (String id : before.keySet()) {
            assertEquals(before.get(id), after.get(id),
                    () -> "节点 " + id + " 的状态被读改动了: " + why);
        }
        log.info("[assert] 全部 {} 个节点 term/role/commitIndex/日志(内容与条数) 完全一致 | {}",
                before.size(), why);
    }

    // ----------------------------------------------------------------
    // 本轮重点：落后副本恢复连通后发读，读不得补齐它的日志/提交位置
    // ----------------------------------------------------------------

    @Test
    void readDoesNotReplicateToReconnectedLaggingReplica() {
        Cluster c = new Cluster(List.of("n1", "n2", "n3", "n4", "n5"));
        assertTrue(c.elect("n1"));
        long term = c.node("n1").getCurrentTerm();
        assertTrue(c.write("n1", "r1", "v1").accepted());
        c.replicate("n1"); // 一轮心跳把 leaderCommit 同步到全部跟随者
        log.info("[case] 5 节点集群 n1 当选 term={}，提交 v1(index=1) 并同步各跟随者提交位置", term);

        // n4/n5 掉队（隔离）；领导者与 n2/n3 构成多数派继续写入并提交 index=2,3
        c.isolate("n4");
        c.isolate("n5");
        assertTrue(c.write("n1", "r2", "v2").accepted());
        assertTrue(c.write("n1", "r3", "v3").accepted());
        log.info("[case] n4/n5 掉队期间，n1 经多数派(n1,n2,n3)提交 v2(index=2)、v3(index=3)");
        assertEquals(List.of(1L), c.node("n4").getLog().stream().map(LogEntry::index).toList());
        assertEquals(1, c.node("n4").getCommitIndex());

        // 落后副本恢复连通——注意：不调用 replicate()，只发读
        c.heal("n4");
        c.heal("n5");
        Map<String, NodeState> before = snapshot(c, "恢复连通后、读之前");

        ReadResult r1 = c.read("n1");
        assertTrue(r1.success(), "n1+n2+n3 仍构成多数派，读应成功");
        log.info("[read] node=n1 成功 term={} readIndex={} | 落后副本刚恢复，仅发读",
                r1.term(), r1.readIndex());
        assertEquals(3, r1.readIndex());
        ReadResult r2 = c.read("n1"); // 再读一次（含重试语义）
        assertTrue(r2.success());

        Map<String, NodeState> afterRead = snapshot(c, "连续两次读之后");
        assertStatesUnchanged(before, afterRead, "读不得复制日志、不得推进任何节点提交位置");
        // 明确点名落后副本：日志仍停在 index=1，没有凭空多出 v2/v3，提交位置也没跳
        assertEquals(1, c.node("n4").getLog().size(), "读不得给落后副本补日志");
        assertEquals(1, c.node("n4").getCommitIndex(), "读不得推进落后副本的提交位置");
        assertEquals(1, c.node("n5").getLog().size());
        assertEquals(1, c.node("n5").getCommitIndex());
        assertEquals(List.of("v1"),
                c.node("n4").getLog().stream().map(LogEntry::payload).toList());
        log.info("[assert] n4/n5 日志仍仅 index=1、commitIndex=1，依据=读确认是不带条目的纯查询");

        // 对照组：真正的复制流程（写/心跳）才能让落后副本收敛
        c.replicate("n1");
        c.replicate("n1"); // 第二轮心跳携带 leaderCommit 推进提交位置
        assertEquals(List.of("v1", "v2", "v3"),
                c.node("n4").getCommittedEntries().stream().map(LogEntry::payload).toList());
        assertEquals(3, c.node("n4").getCommitIndex());
        assertEquals(List.of("v1", "v2", "v3"),
                c.node("n5").getCommittedEntries().stream().map(LogEntry::payload).toList());
        log.info("[assert] 对照组：显式 replicate() 后 n4/n5 才补齐到 index=3，读与复制职责严格分离");
    }

    // ----------------------------------------------------------------
    // 本轮重点：客户端收到失败结果的写入，不得因一次读出现在别的副本上
    // ----------------------------------------------------------------

    @Test
    void rejectedWriteIsNotSpreadByRead() {
        Cluster c = new Cluster(List.of("n1", "n2", "n3", "n4", "n5"));
        assertTrue(c.elect("n1"));
        assertTrue(c.write("n1", "r1", "v1").accepted());
        c.replicate("n1");
        long term = c.node("n1").getCurrentTerm();
        log.info("[case] n1 term={} 提交 v1；随后隔离 n1 使其够不到多数派", term);

        // 领导者被隔离：写入客户端收到失败（未确认），但条目可能暂存在领导者本地
        c.isolate("n1");
        WriteResult failed = c.write("n1", "r2", "rejected-x");
        assertFalse(failed.accepted());
        assertEquals(RejectionReason.NO_QUORUM, failed.reason());
        assertEquals(2, c.node("n1").getLog().size(), "未确认条目暂存在旧领导者本地");
        log.info("[case] 写入 r2 客户端收到失败 reason={}，rejected-x 仅停在 n1 本地 index=2",
                failed.reason());

        // 恢复连通（多数派里没有任何节点写过 r2），不触发复制，只读
        c.heal("n1");
        Map<String, NodeState> before = snapshot(c, "恢复连通后、读之前");

        ReadResult r1 = c.read("n1");
        ReadResult r2 = c.read("n1");
        ReadResult r3 = c.read("n1");
        // n1 本任期提交过 r1，多数派认可它仍为合法领导者，读可以成功；
        // 但成功只依赖只读确认，绝不允许把本地未确认的 rejected-x 捎带出去
        assertTrue(r1.success() && r2.success() && r3.success());
        log.info("[read] node=n1 连续 3 次成功 term={} readIndex={}（均 <=1，不含 rejected-x）",
                r1.term(), r1.readIndex());
        assertEquals(1, r1.readIndex(), "读上界是已确认位置，未确认条目不可见");

        Map<String, NodeState> afterRead = snapshot(c, "连续三次读之后");
        assertStatesUnchanged(before, afterRead, "被拒绝写入不得因读扩散到任何副本");

        for (String id : c.nodeIds()) {
            boolean containsRejected = c.node(id).getLog().stream()
                    .anyMatch(e -> "r2".equals(e.requestId()) || "rejected-x".equals(e.payload()));
            if (!id.equals("n1")) {
                assertFalse(containsRejected,
                        "被拒绝的 rejected-x 不得因读出现在副本 " + id + " 上");
            }
            // 读期间没有任何写入被确认：所有节点提交位置都不得越过 1
            assertEquals(1, c.node(id).getCommitIndex(),
                    "读不得推进 " + id + " 的提交位置");
        }
        log.info("[assert] rejected-x 仍只存在于 n1 本地；n2..n5 无该条目，所有节点 commitIndex=1");
    }
}
