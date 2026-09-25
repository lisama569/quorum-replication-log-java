package com.github.lisama569.quorumreplicationlogjava.tests;

import com.github.lisama569.quorumreplicationlogjava.replication.AppendRequest;
import com.github.lisama569.quorumreplicationlogjava.replication.AppendResponse;
import com.github.lisama569.quorumreplicationlogjava.replication.Cluster;
import com.github.lisama569.quorumreplicationlogjava.replication.LogEntry;
import com.github.lisama569.quorumreplicationlogjava.replication.NodeRole;
import com.github.lisama569.quorumreplicationlogjava.replication.RejectionReason;
import com.github.lisama569.quorumreplicationlogjava.replication.VoteRequest;
import com.github.lisama569.quorumreplicationlogjava.replication.VoteResponse;
import com.github.lisama569.quorumreplicationlogjava.replication.WriteResult;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 多副本复制日志的行为验证：选举、法定人数读写、分区恢复收敛、
 * 重启追赶、幂等重试与并发写入。日志中打印任期、日志索引与判定依据。
 */
class QuorumReplicationTest {

    private static final Logger log = LoggerFactory.getLogger(QuorumReplicationTest.class);
    private static final List<String> IDS = List.of("n1", "n2", "n3");

    private Cluster newCluster() {
        return new Cluster(IDS);
    }

    private void describe(String fmt, Object... args) {
        log.info("[case] " + fmt, args);
    }

    private void dumpNode(Cluster c, String id, String why) {
        var n = c.node(id);
        log.info("[state] node={} role={} term={} commitIndex={} log={} | {}",
                id, n.getRole(), n.getCurrentTerm(), n.getCommitIndex(),
                n.getLog().stream().map(e -> e.index() + "@t" + e.term() + ":" + e.payload()).toList(),
                why);
    }

    private void assertConverged(Cluster c, String why) {
        List<LogEntry> reference = null;
        for (String id : c.nodeIds()) {
            dumpNode(c, id, why);
            List<LogEntry> committed = c.node(id).getCommittedEntries();
            if (reference == null) {
                reference = committed;
            } else {
                assertEquals(reference, committed,
                        () -> "已提交序列不一致: " + id + " vs 参考, " + why);
            }
        }
        log.info("[assert] 全部副本已提交序列一致: {} | {}", reference, why);
    }

    // ----------------------------------------------------------------
    // 选举
    // ----------------------------------------------------------------

    @Test
    void electionSingleLeaderPerTermAndDuplicateVotesSafe() {
        Cluster c = newCluster();
        assertTrue(c.elect("n1"), "n1 应在任期 1 当选");
        assertEquals(NodeRole.LEADER, c.node("n1").getRole());
        describe("n1 当选 term={}", c.node("n1").getCurrentTerm());

        // 同任期重复投票请求（重试）：幂等，仍返回已投结果，不产生第二票
        VoteRequest dup = new VoteRequest(2, "n2", 0, 0);
        VoteResponse first = c.node("n1").handleVoteRequest(dup);
        VoteResponse retry = c.node("n1").handleVoteRequest(dup);
        assertTrue(first.voteGranted() && retry.voteGranted(), "重复投票请求应幂等");
        // 同任期另一候选人不得再获得 n1 的票 -> 同任期不可能双主
        VoteResponse other = c.node("n1").handleVoteRequest(new VoteRequest(2, "n3", 0, 0));
        assertFalse(other.voteGranted(), "同一任期只能投一票");
        log.info("[assert] term=2 n1 已投 n2，拒投 n3，依据=每任期一票且先持久化");

        // 过期任期的投票请求被明确拒绝且原因可区分
        VoteResponse stale = c.node("n1").handleVoteRequest(new VoteRequest(1, "n3", 0, 0));
        assertFalse(stale.voteGranted());
        assertEquals(RejectionReason.STALE_TERM, stale.reason());
        log.info("[assert] 过期任期投票被拒绝 reason={} (请求 term=1 < 当前 term={})",
                stale.reason(), c.node("n1").getCurrentTerm());
    }

    @Test
    void minorityPartitionCannotElectLeaderOrAcceptWrites() {
        Cluster c = newCluster();
        assertTrue(c.elect("n1"));
        WriteResult ok = c.write("n1", "r1", "v1");
        assertTrue(ok.accepted());
        describe("写入 r1 已提交 index={} term={}", ok.index(), c.node("n1").getCurrentTerm());

        // 隔离领导者（少数派分区）：不得接受写入
        c.isolate("n1");
        WriteResult rejected = c.write("n1", "r2", "v2");
        assertFalse(rejected.accepted());
        assertEquals(RejectionReason.NO_QUORUM, rejected.reason());
        log.info("[assert] 少数派分区写入被拒绝 reason={} index={}，依据=未获多数派确认",
                rejected.reason(), rejected.index());

        // 少数派分区中的选举无法成功（收不到票）
        assertFalse(c.elect("n1"), "被隔离节点不应当选");

        // 多数派一侧可另选领导者并继续服务
        assertTrue(c.elect("n2"));
        WriteResult ok2 = c.write("n2", "r3", "v3");
        assertTrue(ok2.accepted());
        describe("多数派侧新领导者 n2 term={} 写入 r3 index={}",
                c.node("n2").getCurrentTerm(), ok2.index());
    }

    // ----------------------------------------------------------------
    // 法定人数读写与副本收敛
    // ----------------------------------------------------------------

    @Test
    void partitionHealsAndReplicasConvergeWithoutLosingCommittedWrites() {
        Cluster c = newCluster();
        assertTrue(c.elect("n1"));
        assertTrue(c.write("n1", "r1", "v1").accepted());

        // 隔离 n3，多数派 (n1,n2) 继续写入
        c.isolate("n3");
        assertTrue(c.write("n1", "r2", "v2").accepted());
        assertTrue(c.write("n1", "r3", "v3").accepted());
        assertEquals(1, c.node("n3").getLog().size(), "n3 被隔离，不应收到新条目");
        describe("n3 隔离期间提交 r2,r3；n3 日志停留在 index={}",
                c.node("n3").getLog().get(0).index());

        // 分区恢复：领导者心跳/复制使 n3 收敛
        c.heal("n3");
        c.replicate("n1");
        c.replicate("n1"); // 第二轮携带最新 leaderCommit 推进 n3 提交索引
        assertConverged(c, "分区恢复后 n3 补齐日志并收敛");
        assertEquals(List.of("v1", "v2", "v3"),
                c.node("n3").getCommittedEntries().stream().map(LogEntry::payload).toList());
    }

    @Test
    void conflictingUncommittedEntriesAreTruncatedCommittedOnesSurvive() {
        Cluster c = newCluster();
        assertTrue(c.elect("n1"));
        assertTrue(c.write("n1", "r1", "committed-1").accepted());

        // n1 被隔离后写入：未达多数派，条目仅留在 n1 本地（未提交）
        c.isolate("n1");
        WriteResult w = c.write("n1", "r2", "uncommitted-x");
        assertFalse(w.accepted());
        assertEquals(RejectionReason.NO_QUORUM, w.reason());
        assertEquals(2, c.node("n1").getLog().size(), "未提交条目暂存于旧领导者本地");
        dumpNode(c, "n1", "旧领导者持有未提交冲突条目 index=2");

        // 多数派侧选出新领导者并提交同索引的不同条目
        assertTrue(c.elect("n2"));
        assertTrue(c.write("n2", "r3", "committed-2").accepted());
        dumpNode(c, "n2", "新领导者在 index=2 提交 committed-2");

        // 恢复分区：旧领导者退位，未提交冲突条目被截断，已提交条目保留
        c.heal("n1");
        c.replicate("n2");
        c.replicate("n2");
        assertEquals(NodeRole.FOLLOWER, c.node("n1").getRole());
        assertConverged(c, "冲突解决：截断未提交后缀，保留已提交条目");
        assertEquals(List.of("committed-1", "committed-2"),
                c.node("n1").getCommittedEntries().stream().map(LogEntry::payload).toList());
        log.info("[assert] 已确认写入 committed-1 未丢失；未确认的 uncommitted-x 被确定性截断");
    }

    @Test
    void staleTermAndIndexRequestsAreRejectedWithDistinctReasons() {
        Cluster c = newCluster();
        assertTrue(c.elect("n1"));
        assertTrue(c.write("n1", "r1", "v1").accepted());
        long term = c.node("n1").getCurrentTerm();

        // 过期任期的复制请求
        AppendResponse stale = c.node("n2").handleAppendRequest(
                new AppendRequest(term - 1, "n9", 0, 0, List.of(), 0));
        assertFalse(stale.success());
        assertEquals(RejectionReason.STALE_TERM, stale.reason());
        log.info("[assert] 过期任期复制请求被拒绝 reason={} (term={} < {})",
                stale.reason(), term - 1, term);

        // 前导索引越界
        AppendResponse oor = c.node("n2").handleAppendRequest(
                new AppendRequest(term, "n1", 99, term, List.of(), 0));
        assertFalse(oor.success());
        assertEquals(RejectionReason.INDEX_OUT_OF_RANGE, oor.reason());
        log.info("[assert] 前导索引越界被拒绝 reason={} (prevLogIndex=99 > lastIndex={})",
                oor.reason(), oor.matchIndex());

        // 前导任期不匹配（日志冲突）
        AppendResponse conflict = c.node("n2").handleAppendRequest(
                new AppendRequest(term, "n1", 1, term + 5, List.of(), 0));
        assertFalse(conflict.success());
        assertEquals(RejectionReason.LOG_CONFLICT, conflict.reason());
        log.info("[assert] 前导任期不匹配被拒绝 reason={} (prevLogTerm={} != 本地 term={})",
                conflict.reason(), term + 5, term);
    }

    // ----------------------------------------------------------------
    // 重启与幂等
    // ----------------------------------------------------------------

    @Test
    void restartedNodeCatchesUpAndCommittedWritesSurvive() {
        Cluster c = newCluster();
        assertTrue(c.elect("n1"));
        assertTrue(c.write("n1", "r1", "v1").accepted());
        assertTrue(c.write("n1", "r2", "v2").accepted());

        // 重启跟随者 n3：磁盘保留，易失状态（角色/提交索引）丢失
        c.restart("n3");
        assertEquals(NodeRole.FOLLOWER, c.node("n3").getRole());
        assertEquals(2, c.node("n3").getLog().size(), "重启后日志从磁盘恢复");
        assertEquals(0, c.node("n3").getCommitIndex(), "提交索引为易失状态，重启归零");
        dumpNode(c, "n3", "重启后：日志保留、提交索引待领导者心跳推进");

        c.replicate("n1");
        assertEquals(2, c.node("n3").getCommitIndex(), "心跳携带 leaderCommit 后补齐提交索引");
        assertConverged(c, "重启节点补齐日志");

        // 重启领导者：已提交条目不丢失，重新当选后继续服务
        c.restart("n1");
        assertEquals(2, c.node("n1").getLog().size());
        assertTrue(c.elect("n1"), "重启后可凭完整日志重新当选");
        assertTrue(c.write("n1", "r3", "v3").accepted());
        c.replicate("n1");
        assertConverged(c, "领导者重启后已确认写入不丢失");
    }

    @Test
    void duplicateRequestIdIsAppliedOnlyOnce() {
        Cluster c = newCluster();
        assertTrue(c.elect("n1"));
        WriteResult first = c.write("n1", "req-42", "payload");
        assertTrue(first.accepted());

        // 相同请求标识重试：返回相同索引，不重复应用
        WriteResult retry = c.write("n1", "req-42", "payload");
        assertTrue(retry.accepted());
        assertEquals(first.index(), retry.index());
        assertNull(retry.reason());
        c.replicate("n1");
        for (String id : IDS) {
            long count = c.node(id).getCommittedEntries().stream()
                    .filter(e -> e.requestId().equals("req-42")).count();
            assertEquals(1, count, id + " 上 req-42 只能应用一次");
        }
        log.info("[assert] 幂等重试：req-42 两次提交均返回 index={}，各副本仅应用一次", first.index());
    }

    // ----------------------------------------------------------------
    // 并发写入
    // ----------------------------------------------------------------

    @Test
    void concurrentWritesGetUniqueOrderedIndexesAndConverge() throws InterruptedException {
        Cluster c = newCluster();
        assertTrue(c.elect("n1"));
        int writers = 8;
        int perWriter = 5;
        ExecutorService pool = Executors.newFixedThreadPool(writers);
        CountDownLatch start = new CountDownLatch(1);
        Set<Long> indexes = ConcurrentHashMap.newKeySet();
        List<String> failures = new ArrayList<>();
        for (int w = 0; w < writers; w++) {
            final int wid = w;
            pool.submit(() -> {
                try {
                    start.await();
                    for (int k = 0; k < perWriter; k++) {
                        WriteResult r = c.write("n1", "w" + wid + "-" + k, "data-" + wid + "-" + k);
                        if (!r.accepted()) {
                            synchronized (failures) {
                                failures.add("w" + wid + "-" + k + ":" + r.reason());
                            }
                        } else {
                            indexes.add(r.index());
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS));
        assertTrue(failures.isEmpty(), () -> "并发写入不应失败: " + failures);
        assertEquals(writers * perWriter, indexes.size(), "每个已确认写入应获得唯一日志索引");
        describe("并发写入完成：{} 个写入获得唯一索引，最大 index={} term={}",
                indexes.size(), indexes.stream().mapToLong(Long::longValue).max().orElse(-1),
                c.node("n1").getCurrentTerm());

        c.replicate("n1");
        assertConverged(c, "并发写入后全部副本收敛到同一日志序列");
        // 已提交序列索引连续且从 1 开始
        List<LogEntry> committed = c.node("n1").getCommittedEntries();
        for (int i = 0; i < committed.size(); i++) {
            assertEquals(i + 1, committed.get(i).index());
        }
        assertNotEquals(0, committed.size());
    }
}
