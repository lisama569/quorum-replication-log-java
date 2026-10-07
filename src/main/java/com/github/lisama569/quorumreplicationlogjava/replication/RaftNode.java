package com.github.lisama569.quorumreplicationlogjava.replication;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * 复制日志节点（Raft 风格：领导者选举 + 多数派提交 + 日志复制）。
 *
 * <p>一致性要点：
 * <ul>
 *   <li>任期与投票先持久化再对外响应，保证同一任期最多投出一票；</li>
 *   <li>仅当日志不比候选人旧时才投票，保证当选者包含全部已提交条目；</li>
 *   <li>写入仅在多数派复制完成（提交）后才对客户端确认；</li>
 *   <li>冲突处理：仅截断未提交的后缀，已提交条目永不覆盖；</li>
 *   <li>幂等：按 requestId 去重，重复提交返回首次的日志索引。</li>
 * </ul>
 * 单节点内部状态由 synchronized 保护；网络调用在锁外进行，避免死锁。
 */
public class RaftNode {

    private final String nodeId;
    private final int clusterSize;
    private final DurableStore store;

    // ---- 持久化状态（写时同步落盘到 store）----
    private long currentTerm;
    private String votedFor;
    private final List<LogEntry> log;

    // ---- 易失状态（重启丢失，可安全重建）----
    private NodeRole role = NodeRole.FOLLOWER;
    private long commitIndex;
    private String knownLeaderId;
    /** 仅领导者使用：每个副本下一条待发送的日志索引。 */
    private final Map<String, Long> nextIndex = new HashMap<>();

    public RaftNode(String nodeId, int clusterSize, DurableStore store) {
        this.nodeId = nodeId;
        this.clusterSize = clusterSize;
        this.store = store;
        // 从持久化存储恢复（模拟重启后读盘）
        this.currentTerm = store.loadCurrentTerm();
        this.votedFor = store.loadVotedFor();
        this.log = new ArrayList<>(store.loadLog());
    }

    public String getNodeId() {
        return nodeId;
    }

    public synchronized NodeRole getRole() {
        return role;
    }

    public synchronized long getCurrentTerm() {
        return currentTerm;
    }

    public synchronized long getCommitIndex() {
        return commitIndex;
    }

    public synchronized String getKnownLeaderId() {
        return knownLeaderId;
    }

    /** 返回当前日志快照（含未提交条目）。 */
    public synchronized List<LogEntry> getLog() {
        return new ArrayList<>(log);
    }

    /** 返回已提交（多数派确认、对外可见）的条目序列。 */
    public synchronized List<LogEntry> getCommittedEntries() {
        return new ArrayList<>(log.subList(0, (int) Math.min(commitIndex, log.size())));
    }

    private int majority() {
        return clusterSize / 2 + 1;
    }

    private synchronized long lastLogIndex() {
        return log.isEmpty() ? 0 : log.get(log.size() - 1).index();
    }

    private synchronized long lastLogTerm() {
        return log.isEmpty() ? 0 : log.get(log.size() - 1).term();
    }

    /** 持久化任期与投票（必须先落盘再响应，保证崩溃重启后不重复投票）。 */
    private synchronized void persistTermAndVote() {
        store.saveCurrentTerm(currentTerm);
        store.saveVotedFor(votedFor);
    }

    private synchronized void persistLog() {
        store.saveLog(log);
    }

    /** 发现更高任期（或确认同任期领导者）时退位为跟随者；仅任期变大才清空投票。 */
    private synchronized void stepDown(long newTerm) {
        role = NodeRole.FOLLOWER;
        if (newTerm > currentTerm) {
            currentTerm = newTerm;
            votedFor = null;
        }
        persistTermAndVote();
    }

    // ------------------------------------------------------------------
    // 选举
    // ------------------------------------------------------------------

    /**
     * 发起选举。同一任期多数派只能投出一个领导者；重复投票/重试因
     * “每任期一票 + 先持久化”而不会产生双主。
     *
     * @return 是否当选
     */
    public boolean startElection(Transport transport, List<String> peers) {
        final long term;
        final long lastIdx;
        final long lastTerm;
        synchronized (this) {
            role = NodeRole.CANDIDATE;
            currentTerm++;
            votedFor = nodeId;
            persistTermAndVote();
            term = currentTerm;
            lastIdx = lastLogIndex();
            lastTerm = lastLogTerm();
        }
        VoteRequest request = new VoteRequest(term, nodeId, lastIdx, lastTerm);
        int votes = 1; // 自己一票
        for (String peer : peers) {
            Optional<VoteResponse> resp = transport.sendVoteRequest(peer, request);
            if (resp.isEmpty()) {
                continue; // 分区/宕机：不可达
            }
            if (resp.get().term() > term) {
                stepDown(resp.get().term());
                return false;
            }
            if (resp.get().voteGranted()) {
                votes++;
            }
        }
        synchronized (this) {
            // 任期已变（发现了更高任期）则本次选举作废
            if (role == NodeRole.CANDIDATE && currentTerm == term && votes >= majority()) {
                role = NodeRole.LEADER;
                knownLeaderId = nodeId;
                for (String peer : peers) {
                    nextIndex.put(peer, lastLogIndex() + 1);
                }
                return true;
            }
            return false;
        }
    }

    /** 处理投票请求。过期任期拒绝（STALE_TERM）；日志落后或已投他人则拒投。 */
    public synchronized VoteResponse handleVoteRequest(VoteRequest request) {
        if (request.term() < currentTerm) {
            return new VoteResponse(currentTerm, false, RejectionReason.STALE_TERM);
        }
        if (request.term() > currentTerm) {
            stepDown(request.term());
        }
        boolean logUpToDate = request.lastLogTerm() > lastLogTerm()
                || (request.lastLogTerm() == lastLogTerm() && request.lastLogIndex() >= lastLogIndex());
        // 重复投票请求幂等：已投给该候选人则再次确认
        boolean canVote = votedFor == null || votedFor.equals(request.candidateId());
        if (canVote && logUpToDate) {
            votedFor = request.candidateId();
            persistTermAndVote();
            return new VoteResponse(currentTerm, true, null);
        }
        return new VoteResponse(currentTerm, false, null);
    }

    // ------------------------------------------------------------------
    // 日志复制
    // ------------------------------------------------------------------

    /**
     * 处理日志复制请求（含心跳）。
     * 过期任期拒绝（STALE_TERM）；前导索引越界拒绝（INDEX_OUT_OF_RANGE）；
     * 前导任期不匹配拒绝（LOG_CONFLICT）。冲突时仅截断未提交后缀，
     * 已提交条目绝不覆盖或丢弃。
     */
    public synchronized AppendResponse handleAppendRequest(AppendRequest request) {
        if (request.term() < currentTerm) {
            return new AppendResponse(currentTerm, false, RejectionReason.STALE_TERM, lastLogIndex());
        }
        if (request.term() > currentTerm || role != NodeRole.FOLLOWER) {
            stepDown(request.term());
        }
        knownLeaderId = request.leaderId();

        // 前导一致性检查
        if (request.prevLogIndex() > lastLogIndex()) {
            return new AppendResponse(currentTerm, false, RejectionReason.INDEX_OUT_OF_RANGE, lastLogIndex());
        }
        if (request.prevLogIndex() > 0
                && log.get((int) request.prevLogIndex() - 1).term() != request.prevLogTerm()) {
            return new AppendResponse(currentTerm, false, RejectionReason.LOG_CONFLICT, lastLogIndex());
        }

        // 追加/冲突截断：逐条比对，仅在任期不一致时截断未提交后缀
        boolean changed = false;
        for (LogEntry entry : request.entries()) {
            int i = (int) entry.index() - 1;
            if (i < log.size()) {
                LogEntry existing = log.get(i);
                if (existing.term() != entry.term()) {
                    if (entry.index() <= commitIndex) {
                        // 防御：已提交条目永不覆盖（正确的多数派选举不会触发此分支）
                        return new AppendResponse(currentTerm, false, RejectionReason.LOG_CONFLICT, lastLogIndex());
                    }
                    log.subList(i, log.size()).clear();
                    log.add(entry);
                    changed = true;
                }
                // 任期相同视为同一条目（重发幂等），跳过
            } else {
                log.add(entry);
                changed = true;
            }
        }
        if (changed) {
            persistLog();
        }
        // 推进提交索引：不超过本地最后一条日志
        long newCommit = Math.min(request.leaderCommit(), lastLogIndex());
        if (newCommit > commitIndex) {
            commitIndex = newCommit;
        }
        return new AppendResponse(currentTerm, true, null, lastLogIndex());
    }

    // ------------------------------------------------------------------
    // 客户端写入
    // ------------------------------------------------------------------

    /**
     * 客户端写入。仅领导者受理；多数派确认后返回成功，否则以
     * {@link RejectionReason#NO_QUORUM} 拒绝。相同 requestId 的重复提交
     * 幂等返回首次写入的索引，不会重复应用。
     */
    public WriteResult clientWrite(Transport transport, List<String> peers, String requestId, String payload) {
        synchronized (this) {
            if (role != NodeRole.LEADER) {
                return WriteResult.rejected(RejectionReason.NOT_LEADER, knownLeaderId);
            }
            // 幂等：该请求已写入过日志
            LogEntry existing = findByRequestId(requestId);
            if (existing != null) {
                if (existing.index() <= commitIndex) {
                    return WriteResult.accepted(existing.index(), nodeId);
                }
                // 已记录但未提交：继续走复制流程尝试提交
            } else {
                log.add(new LogEntry(currentTerm, lastLogIndex() + 1, requestId, payload));
                persistLog();
            }
        }
        replicate(transport, peers);
        synchronized (this) {
            LogEntry entry = findByRequestId(requestId);
            if (role == NodeRole.LEADER && entry != null && entry.index() <= commitIndex) {
                return WriteResult.accepted(entry.index(), nodeId);
            }
            if (role != NodeRole.LEADER) {
                return WriteResult.rejected(RejectionReason.NOT_LEADER, knownLeaderId);
            }
            return WriteResult.rejected(RejectionReason.NO_QUORUM, nodeId);
        }
    }

    private synchronized LogEntry findByRequestId(String requestId) {
        for (LogEntry e : log) {
            if (e.requestId().equals(requestId)) {
                return e;
            }
        }
        return null;
    }

    // ------------------------------------------------------------------
    // 客户端一致性读（ReadIndex 风格）
    // ------------------------------------------------------------------

    /**
     * 处理读前领导权确认（ReadIndex 流程的只读 RPC）。
     *
     * <p><b>严格无副作用</b>：不追加/截断日志、不推进 commitIndex、不改任期、
     * 不改角色、不更新已知领导者、不落盘。它不承担心跳的任何职责（心跳推进
     * 提交位置的副作用只能来自真正的复制流程 {@link #replicate}）。
     *
     * <p>认可规则：请求任期不旧于本节点当前任期即 grant（过期领导者的请求
     * 任期更小，必然被多数派拒绝）；响应回带本节点当前任期，供发起方发现
     * 更高任期后自行退位（退位是发起方读失败路径上的本地修正，不是响应方
     * 对本 RPC 做出的状态改变）。
     */
    public synchronized ReadConfirmResponse handleReadConfirm(ReadConfirmRequest request) {
        boolean grant = request.term() >= currentTerm;
        return new ReadConfirmResponse(currentTerm, grant);
    }

    /** 当前任期是否已有已提交条目（领导者能否确定自己的提交索引是最新的）。 */
    private synchronized boolean hasCommittedEntryInCurrentTerm() {
        return commitIndex > 0 && log.get((int) commitIndex - 1).term() == currentTerm;
    }

    /**
     * 客户端一致性读。只返回已经多数派确认的条目，且保证读到发起读之前
     * 已确认的全部写入（线性一致）。流程：
     * <ol>
     *   <li>仅领导者受理；非领导者以 {@link RejectionReason#NOT_LEADER} 失败
     *       （附已知领导者提示）；</li>
     *   <li>领导者须已在当前任期提交过条目，否则无法确认自己的提交索引最新，
     *       以 {@link RejectionReason#NO_LEADER} 失败（选举刚结束的窗口）；</li>
     *   <li>向各副本发一轮<b>只读确认</b> {@link ReadConfirmRequest}（不含任何
     *       日志条目、不含 leaderCommit）验证领导权仍然有效：该消息在发送方与
     *       响应方都<b>零副作用</b>——不追加/截断日志、不推进提交索引、不改
     *       nextIndex、不改任期与角色；</li>
     *   <li>够不到多数派（不可达/不认可）以 {@link RejectionReason#NO_QUORUM}
     *       失败，确认在时限之后才凑齐以 {@link RejectionReason#READ_CONFIRM_TIMEOUT}
     *       失败——被隔离的旧领导者因此不会把本地旧数据当成最新结果返回；</li>
     *   <li>确认通过后以当前 commitIndex 为 readIndex，返回 [1, readIndex]
     *       的已提交条目快照与确认时的任期。</li>
     * </ol>
     * 无论成功还是失败、无论重试多少次，读都不改变任何节点（含本节点）的
     * 日志内容、条目数量、已提交位置、任期与角色。
     */
    public ReadResult clientRead(Transport transport, List<String> peers, long confirmTimeoutMillis) {
        final long term;
        final long readIndex;
        synchronized (this) {
            if (role != NodeRole.LEADER) {
                return ReadResult.failed(RejectionReason.NOT_LEADER, knownLeaderId);
            }
            if (!hasCommittedEntryInCurrentTerm()) {
                // 新当选尚未提交本任期条目：提交索引可能落后于集群真实已提交位置
                return ReadResult.failed(RejectionReason.NO_LEADER, nodeId);
            }
            term = currentTerm;
            readIndex = commitIndex;
        }

        ReadConfirmRequest confirm = new ReadConfirmRequest(term, nodeId);
        long deadline = System.nanoTime() + confirmTimeoutMillis * 1_000_000L;
        int acks = 1; // 自己一票
        long quorumAt = acks >= majority() ? System.nanoTime() : -1;
        for (String peer : peers) {
            if (acks >= majority() || System.nanoTime() > deadline) {
                break; // 已凑齐多数派，或时限已到不再发起新的确认
            }
            Optional<ReadConfirmResponse> resp = transport.sendReadConfirm(peer, confirm);
            if (resp.isEmpty()) {
                continue; // 不可达：不计入确认
            }
            if (resp.get().term() > term) {
                // 发现更高任期：领导者身份已过期，本地退位并失败（仅此一处本地修正）
                stepDown(resp.get().term());
                return ReadResult.failed(RejectionReason.NO_LEADER, knownLeaderId);
            }
            if (resp.get().grant()) {
                acks++;
                if (acks >= majority()) {
                    quorumAt = System.nanoTime();
                    break; // 多数派已确认，无需等待其余副本
                }
            }
        }
        if (acks < majority()) {
            // 始终没凑齐多数派：分区/宕机（不可达即时返回，不会拖到时限）
            return ReadResult.failed(RejectionReason.NO_QUORUM, nodeId);
        }
        if (quorumAt > deadline) {
            // 最后一票把多数派凑齐时已经越过时限（慢副本迟迟不回）：按超时失败，
            // 与“始终够不到多数派”的 NO_QUORUM 明确区分
            return ReadResult.failed(RejectionReason.READ_CONFIRM_TIMEOUT, nodeId);
        }
        synchronized (this) {
            if (role != NodeRole.LEADER || currentTerm != term) {
                return ReadResult.failed(RejectionReason.NO_LEADER, knownLeaderId);
            }
            if (commitIndex < readIndex) {
                // 防御性检查：提交索引只会单调前进；若异常回退则不返回数据
                return ReadResult.failed(RejectionReason.NO_LEADER, knownLeaderId);
            }
            // 返回确认时刻的 readIndex 上界（不超过当前 commitIndex）
            long snapshotIndex = Math.min(readIndex, commitIndex);
            return ReadResult.ok(term, snapshotIndex,
                    log.subList(0, (int) snapshotIndex), nodeId);
        }
    }

    /**
     * 领导者向所有可达副本复制日志并推进提交索引。
     * 冲突时按 nextIndex 逐条回退重试，直到与副本日志匹配。
     */
    public void replicate(Transport transport, List<String> peers) {
        final long term;
        synchronized (this) {
            if (role != NodeRole.LEADER) {
                return;
            }
            term = currentTerm;
        }
        for (String peer : peers) {
            while (true) {
                final AppendRequest request;
                synchronized (this) {
                    if (role != NodeRole.LEADER || currentTerm != term) {
                        return;
                    }
                    long next = nextIndex.getOrDefault(peer, lastLogIndex() + 1);
                    long prevIndex = next - 1;
                    long prevTerm = prevIndex > 0 ? log.get((int) prevIndex - 1).term() : 0;
                    List<LogEntry> entries = new ArrayList<>(
                            log.subList((int) next - 1, log.size()));
                    request = new AppendRequest(term, nodeId, prevIndex, prevTerm, entries, commitIndex);
                }
                Optional<AppendResponse> resp = transport.sendAppendRequest(peer, request);
                if (resp.isEmpty()) {
                    break; // 不可达：本轮放弃该副本
                }
                if (resp.get().term() > term) {
                    stepDown(resp.get().term());
                    return;
                }
                if (resp.get().success()) {
                    synchronized (this) {
                        nextIndex.put(peer, resp.get().matchIndex() + 1);
                    }
                    break;
                }
                synchronized (this) {
                    long next = nextIndex.getOrDefault(peer, 1L);
                    if (next <= 1) {
                        break; // 已回退到起点仍失败，等待下一轮
                    }
                    nextIndex.put(peer, next - 1);
                }
            }
        }
        advanceCommitIndex(term);
    }

    /** 仅提交当前任期内、已复制到多数派的条目（Raft 安全提交规则）。 */
    private synchronized void advanceCommitIndex(long leaderTerm) {
        if (role != NodeRole.LEADER || currentTerm != leaderTerm) {
            return;
        }
        for (long i = lastLogIndex(); i > commitIndex; i--) {
            LogEntry e = log.get((int) i - 1);
            if (e.term() != currentTerm) {
                continue;
            }
            int replicated = 1; // 自己
            for (long next : nextIndex.values()) {
                if (next > i) {
                    replicated++;
                }
            }
            if (replicated >= majority()) {
                commitIndex = i;
                break;
            }
        }
    }
}
