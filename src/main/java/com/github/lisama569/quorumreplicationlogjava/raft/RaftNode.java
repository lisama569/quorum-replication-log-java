package com.github.lisama569.quorumreplicationlogjava.raft;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 单个 Raft 副本节点（进程内模拟，方法均在节点锁内串行执行，支持多客户端线程并发提交）。
 *
 * <p>实现 Raft 论文（2014 年扩展版）的核心规则：
 * <ul>
 *   <li>选举安全：同一任期最多一个领导者；同一任期一票；候选人日志至少一样新才得票。</li>
 *   <li>多数派提交：只有当前任期的条目由多数派复制后才能提升 commitIndex（避免 Figure 8 问题）。</li>
 *   <li>冲突回溯：AppendedEntries 失败时按冲突任期/索引快速回退 nextIndex。</li>
 *   <li>心跳：领导者周期性空 AppendEntries 维持权威、推进落后副本提交并驱动重传。</li>
 * </ul>
 */
public class RaftNode {

    private static final Logger log = LoggerFactory.getLogger(RaftNode.class);

    private final String nodeId;
    private final List<String> peerIds;
    private final int quorum;
    private final Network network;
    private final PersistentState ps;
    private final StateMachine sm;

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition commitUpdated = lock.newCondition();

    private volatile boolean running = false;
    private Role role = Role.FOLLOWER;
    private String leaderId;

    // 选举计时（以 tick 数计，由集群调度器驱动）
    private int electionTimeoutTicks;
    private int ticksSinceReset;
    private int minElectionTicks;
    private int maxElectionTicks;
    private int heartbeatIntervalTicks;
    private int ticksSinceHeartbeat;

    // 领导者易失状态
    private final Map<String, Long> nextIndex = new HashMap<>();
    private final Map<String, Long> matchIndex = new HashMap<>();

    /** 客户端请求幂等表：requestId -> 已确认条目索引（重启后通过扫描日志重建）。 */
    private final Map<String, Long> dedupeIndex = new HashMap<>();
    /** 正在复制的请求 -> 完成信号，使重复提交线程等待同一轮复制而非重复入日志。 */
    private final Map<String, Object> pending = new HashMap<>();

    /** 客户端确认等待的最大 tick 数（由集群按调度周期换算，0 表示用默认值）。 */
    private volatile int submitTimeoutTicks = 100;
    /** 单调递增的节点 tick 计数，供提交等待换算超时；领导者每 tick 唤醒一次等待线程。 */
    private long monotonicTick = 0;

    public RaftNode(String nodeId, List<String> peerIds, Network network,
                    PersistentState persistent, StateMachine stateMachine) {
        this(nodeId, peerIds, network, persistent, stateMachine, 15, 30, 5);
    }

    /**
     * @param minElectionTicks       选举超时下界（tick 数）
     * @param maxElectionTicks       选举超时上界（tick 数，随机区间产生不同的选举超时）
     * @param heartbeatIntervalTicks 心跳间隔（tick 数，须小于选举超时下界）
     */
    public RaftNode(String nodeId, List<String> peerIds, Network network,
                    PersistentState persistent, StateMachine stateMachine,
                    int minElectionTicks, int maxElectionTicks, int heartbeatIntervalTicks) {
        this.nodeId = Objects.requireNonNull(nodeId);
        this.peerIds = List.copyOf(peerIds);
        this.quorum = (peerIds.size() + 1) / 2 + 1;
        this.network = network;
        this.ps = persistent;
        this.sm = stateMachine;
        this.minElectionTicks = minElectionTicks;
        this.maxElectionTicks = maxElectionTicks;
        this.heartbeatIntervalTicks = heartbeatIntervalTicks;
        this.electionTimeoutTicks = randomElectionTimeout();
    }

    private int randomElectionTimeout() {
        return minElectionTicks + (int) (Math.random() * (maxElectionTicks - minElectionTicks + 1));
    }

    // ---------------- 基本访问器 ----------------

    public String id() { return nodeId; }

    public Role role() {
        lock.lock();
        try {
            return role;
        } finally {
            lock.unlock();
        }
    }

    public long term() { return ps.currentTerm(); }

    public long commitIndex() { return ps.commitIndex(); }

    public long lastLogIndex() { return ps.lastLogIndex(); }

    /** 只读快照，供测试核对各副本日志序列是否收敛一致。 */
    public List<LogEntry> logEntries() { return ps.log(); }

    /**
     * 仅供故障注入：在本节点日志末尾直接追加未走复制协议的条目，
     * 用于构造“本地暂存但从未获多数派确认”的分叉日志，验证冲突截断规则。
     */
    public void forceAppendForFaultInjection(List<LogEntry> entries) {
        lock.lock();
        try {
            ps.append(entries);
        } finally {
            lock.unlock();
        }
    }

    public StateMachine stateMachine() { return sm; }

    public boolean isRunning() { return running; }

    public String leaderId() {
        lock.lock();
        try {
            return leaderId;
        } finally {
            lock.unlock();
        }
    }

    // ---------------- 生命周期 ----------------

    public void start() {
        lock.lock();
        try {
            running = true;
            role = Role.FOLLOWER;
            leaderId = null;
            ticksSinceReset = 0;
            ticksSinceHeartbeat = 0;
            monotonicTick = 0;
            // 从持久化日志重建请求幂等表，重启后相同 requestId 仍可识别
            dedupeIndex.clear();
            pending.clear();
            for (LogEntry e : ps.log()) {
                dedupeIndex.putIfAbsent(e.requestId(), e.index());
            }
            // 重启后先把任期内已提交的日志补应用到状态机（状态机为易失状态，按日志重放）
            advanceApplied();
            log.info("[{}] 节点启动 term={} lastLogIndex={} commitIndex={} 已重建幂等表条目数={}",
                    nodeId, term(), lastLogIndex(), commitIndex(), dedupeIndex.size());
        } finally {
            lock.unlock();
        }
    }

    public void shutdown() {
        lock.lock();
        try {
            running = false;
            role = Role.FOLLOWER;
            leaderId = null;
            nextIndex.clear();
            matchIndex.clear();
            pending.clear();
            commitUpdated.signalAll();
            log.info("[{}] 节点停机", nodeId);
        } finally {
            lock.unlock();
        }
    }

    /** 集群设置提交等待的 tick 上限（测试可放大以覆盖慢调度）。 */
    public void setSubmitTimeoutTicks(int ticks) {
        this.submitTimeoutTicks = ticks;
    }

    // ---------------- 定时驱动 ----------------

    public void tick() {
        lock.lock();
        try {
            if (!running) {
                return;
            }
            monotonicTick++;
            if (role == Role.LEADER) {
                ticksSinceHeartbeat++;
                if (ticksSinceHeartbeat >= heartbeatIntervalTicks) {
                    ticksSinceHeartbeat = 0;
                    broadcastAppendEntries();
                }
            } else {
                ticksSinceReset++;
                if (ticksSinceReset >= electionTimeoutTicks) {
                    startElection();
                }
            }
            // 唤醒等待多数派确认的客户端线程，使其检查提交结果与超时
            commitUpdated.signalAll();
        } finally {
            lock.unlock();
        }
    }

    private void resetElectionTimer() {
        ticksSinceReset = 0;
        electionTimeoutTicks = randomElectionTimeout();
    }

    // ---------------- 选举 ----------------

    private void startElection() {
        long newTerm = term() + 1;
        ps.saveTermAndVote(newTerm, nodeId);
        role = Role.CANDIDATE;
        leaderId = null;
        resetElectionTimer();

        long lastIndex = ps.lastLogIndex();
        long lastTerm = ps.lastLogTerm();
        log.info("[{}] 选举超时，成为候选人 term={} lastLogIndex={} lastLogTerm={} 判定依据=任期内未收到合法领导者心跳",
                nodeId, newTerm, lastIndex, lastTerm);

        RequestVoteRequest req = new RequestVoteRequest(newTerm, nodeId, lastIndex, lastTerm);
        int votes = 1; // 自己投自己（持久化 votedFor=self 保证重启后也不会在本任期改投）
        int unavailable = 0;
        for (String peer : peerIds) {
            try {
                RequestVoteResponse resp = network.sendRequestVote(nodeId, peer, req);
                if (resp == null) {
                    continue;
                }
                if (resp.term() > newTerm) {
                    stepDown(resp.term());
                    log.info("[{}] 在投票过程中发现更高任期 term={} 判定依据=网络响应更新任期，退回跟随者",
                            nodeId, resp.term());
                    return;
                }
                if (resp.voteGranted() && term() == newTerm && role == Role.CANDIDATE) {
                    votes++;
                    log.info("[{}] 获得 {} 投票 term={} 当前票数={}", nodeId, peer, newTerm, votes);
                } else {
                    log.info("[{}] 被 {} 拒绝投票 term={} 判定依据={}", nodeId, peer, newTerm, resp.reason());
                }
            } catch (ReplicatedLogException e) {
                unavailable++;
                log.debug("[{}] 拉票到 {} 失败 reason={} message={}", nodeId, peer, e.getReason(), e.getMessage());
            }
        }

        // 统计票数时仍需确认身份未在 RPC 期间改变
        if (role != Role.CANDIDATE || term() != newTerm) {
            return;
        }
        if (votes >= quorum) {
            becomeLeader();
        } else {
            log.info("[{}] term={} 未获多数票 votes={}/{} unreachable={} 判定依据=继续等待选举超时重试",
                    nodeId, newTerm, votes, quorum, unavailable);
        }
    }

    private void becomeLeader() {
        role = Role.LEADER;
        leaderId = nodeId;
        ticksSinceHeartbeat = heartbeatIntervalTicks; // 让下一次 tick 立即发心跳
        long next = ps.lastLogIndex() + 1;
        for (String peer : peerIds) {
            nextIndex.put(peer, next);
            matchIndex.put(peer, 0L);
        }
        log.info("[{}] 当选领导者 term={} 票数多数派达成 nextIndex={} 判定依据=同一任期多数派投票",
                nodeId, term(), next);
        // 标准 Raft：新领导者写入一条当前任期 no-op 条目，复制到多数派后，
        // 之前任期的所有条目随之一并提交，避免跨任期提交歧义（论文 Figure 8）。
        LogEntry noop = new LogEntry(term(), ps.lastLogIndex() + 1, "__noop__:" + nodeId + ":" + term(),
                "__noop__");
        ps.append(List.of(noop));
        dedupeIndex.put(noop.requestId(), noop.index());
        // 立即广播一次：宣告权威、提交 no-op 以便提交之前任期的条目（标准 Raft 做法）
        broadcastAppendEntries();
    }

    private void stepDown(long newTerm) {
        role = Role.FOLLOWER;
        leaderId = null;
        if (newTerm > term()) {
            ps.saveTermAndVote(newTerm, null);
        }
        nextIndex.clear();
        matchIndex.clear();
        resetElectionTimer();
    }

    /**
     * RPC 入口一律使用 tryLock：若本节点正持有自己的锁并向外发起同步 RPC，
     * 对端的入站 RPC 会立即收到“忙”响应而不是阻塞等待，从而避免
     * “持 A 锁等 B 锁、持 B 锁等 A 锁”的跨节点锁环。调用方把忙响应当作
     * 一次丢包，由下一次心跳/选举超时重试。
     */
    public RequestVoteResponse tryHandleRequestVote(RequestVoteRequest req) {
        if (!lock.tryLock()) {
            return new RequestVoteResponse(term(), false, "BUSY");
        }
        try {
            return handleRequestVote(req);
        } finally {
            lock.unlock();
        }
    }

    public AppendEntriesResponse tryHandleAppendEntries(AppendEntriesRequest req) {
        if (!lock.tryLock()) {
            return new AppendEntriesResponse(term(), false, 0, -1, "BUSY");
        }
        try {
            return handleAppendEntries(req);
        } finally {
            lock.unlock();
        }
    }

    public RequestVoteResponse handleRequestVote(RequestVoteRequest req) {
        lock.lock();
        try {
            if (!running) {
                return new RequestVoteResponse(term(), false, "NODE_DOWN");
            }
            if (req.term() < term()) {
                log.info("[{}] 拒绝投票给 {} termReq={} termLocal={} 判定依据=STALE_TERM 过期任期",
                        nodeId, req.candidateId(), req.term(), term());
                return new RequestVoteResponse(term(), false, "STALE_TERM");
            }
            if (req.term() > term()) {
                stepDown(req.term());
            }

            boolean sameCandidate = req.candidateId().equals(ps.votedFor());
            boolean upToDate = isLogAtLeastAsUpToDate(req.lastLogIndex(), req.lastLogTerm());
            boolean grant = (ps.votedFor() == null || sameCandidate) && upToDate;

            if (grant) {
                ps.saveTermAndVote(term(), req.candidateId());
                resetElectionTimer();
                if (role == Role.CANDIDATE && !req.candidateId().equals(nodeId)) {
                    role = Role.FOLLOWER;
                }
                log.info("[{}] 同意投票给 {} term={} 判定依据=本任期未投票或重复同一候选人且候选人日志足够新",
                        nodeId, req.candidateId(), term());
            } else {
                String reason;
                if (ps.votedFor() != null && !sameCandidate) {
                    reason = "ALREADY_VOTED:" + ps.votedFor();
                } else {
                    reason = "LOG_BEHIND(localIndex=" + ps.lastLogIndex() + ",localTerm=" + ps.lastLogTerm() + ")";
                }
                log.info("[{}] 拒绝投票给 {} term={} 判定依据={}", nodeId, req.candidateId(), term(), reason);
                return new RequestVoteResponse(term(), false, reason);
            }
            return new RequestVoteResponse(term(), true, "GRANTED");
        } finally {
            lock.unlock();
        }
    }

    private boolean isLogAtLeastAsUpToDate(long candidateLastIndex, long candidateLastTerm) {
        long localLastTerm = ps.lastLogTerm();
        long localLastIndex = ps.lastLogIndex();
        if (candidateLastTerm != localLastTerm) {
            return candidateLastTerm > localLastTerm;
        }
        return candidateLastIndex >= localLastIndex;
    }

    // ---------------- AppendEntries ----------------

    public AppendEntriesResponse handleAppendEntries(AppendEntriesRequest req) {
        lock.lock();
        try {
            if (!running) {
                return new AppendEntriesResponse(term(), false, 0, -1, "NODE_DOWN");
            }
            if (req.term() < term()) {
                log.info("[{}] 拒绝 AppendEntries 来自 {} termReq={} termLocal={} 判定依据=STALE_TERM 过期任期",
                        nodeId, req.leaderId(), req.term(), term());
                return new AppendEntriesResponse(term(), false, ps.lastLogIndex(), -1, "STALE_TERM");
            }
            if (req.term() > term()) {
                stepDown(req.term());
            }
            // 当前任期：合法领导者的心跳使任何候选/跟随者服从
            role = Role.FOLLOWER;
            leaderId = req.leaderId();
            resetElectionTimer();

            // prevLog 一致性检查
            LogEntry prev = req.prevLogIndex() == 0 ? null : ps.entryAt(req.prevLogIndex());
            if (req.prevLogIndex() > ps.lastLogIndex()) {
                log.info("[{}] 拒绝 AppendEntries prevLogIndex={} > localLast={} 判定依据=LOG_MISMATCH 索引落后",
                        nodeId, req.prevLogIndex(), ps.lastLogIndex());
                return new AppendEntriesResponse(term(), false, ps.lastLogIndex() + 1, -1,
                        "LOG_MISMATCH:PREV_INDEX_MISSING");
            }
            long prevTermLocal = req.prevLogIndex() == 0 ? 0 : prev.term();
            if (prevTermLocal != req.prevLogTerm()) {
                long conflictTerm = prevTermLocal;
                long firstIndexOfTerm = firstIndexOfTerm(conflictTerm);
                log.info("[{}] 拒绝 AppendEntries prevIndex={} prevTermReq={} prevTermLocal={} "
                                + "判定依据=LOG_MISMATCH 任期不一致，建议回退到 index={}",
                        nodeId, req.prevLogIndex(), req.prevLogTerm(), prevTermLocal,
                        firstIndexOfTerm - 1);
                return new AppendEntriesResponse(term(), false, firstIndexOfTerm - 1, conflictTerm,
                        "LOG_MISMATCH:PREV_TERM");
            }

            if (!req.entries().isEmpty()) {
                ps.append(req.entries());
                LogEntry last = req.entries().get(req.entries().size() - 1);
                log.info("[{}] 追加日志 fromIndex={} toIndex={} term={} localLast={} 判定依据=prevLog 一致性通过",
                        nodeId, req.entries().get(0).index(), last.index(), req.term(), ps.lastLogIndex());
            }

            if (req.leaderCommit() > ps.commitIndex()) {
                long newCommit = Math.min(req.leaderCommit(), ps.lastLogIndex());
                if (newCommit > ps.commitIndex()) {
                    ps.saveCommitIndex(newCommit);
                    advanceApplied();
                    log.info("[{}] 跟随领导者推进 commitIndex={} term={} 判定依据=leaderCommit 与本地日志取小",
                            nodeId, newCommit, term());
                }
            }
            return new AppendEntriesResponse(term(), true, ps.lastLogIndex(), -1, "OK");
        } finally {
            lock.unlock();
        }
    }

    private long firstIndexOfTerm(long targetTerm) {
        List<LogEntry> all = ps.log();
        for (int i = 0; i < all.size(); i++) {
            if (all.get(i).term() == targetTerm) {
                return i + 1L;
            }
        }
        return 1;
    }

    private void advanceApplied() {
        // 调用方持锁
        long committed = ps.commitIndex();
        while (sm.lastApplied() < committed) {
            long idx = sm.lastApplied() + 1;
            LogEntry e = ps.entryAt(idx);
            if (e == null) {
                break;
            }
            sm.apply(e);
        }
    }

    // ---------------- 领导者复制 ----------------

    /**
     * 向所有跟随者发送与其匹配的 AppendEntries（心跳或日志重传/复制）。
     * 调用方持锁；RPC 为进程内同步调用，网络层保证不会回环到本节点。
     */
    private void broadcastAppendEntries() {
        // 调用方持锁
        for (String peer : peerIds) {
            replicateTo(peer);
        }
        advanceLeaderCommit();
        advanceApplied();
    }

    private void replicateTo(String peer) {
        // 调用方持锁
        long next = nextIndex.getOrDefault(peer, ps.lastLogIndex() + 1);
        long prevIndex = next - 1;
        long prevTerm = prevIndex == 0 ? 0 : ps.entryAt(prevIndex).term();
        List<LogEntry> batch = new ArrayList<>();
        for (long i = next; i <= ps.lastLogIndex(); i++) {
            batch.add(ps.entryAt(i));
        }
        AppendEntriesRequest req = new AppendEntriesRequest(
                term(), nodeId, prevIndex, prevTerm, batch, ps.commitIndex());
        try {
            AppendEntriesResponse resp = network.sendAppendEntries(nodeId, peer, req);
            if (resp == null) {
                return;
            }
            if (resp.term() > term()) {
                stepDown(resp.term());
                log.info("[{}] 复制到 {} 时发现更高任期 term={}，退回跟随者", nodeId, peer, resp.term());
                return;
            }
            if (role != Role.LEADER || term() != req.term()) {
                return;
            }
            if (resp.success()) {
                long matched = resp.conflictIndex(); // 成功时携带对端最新索引
                if (matched > matchIndex.getOrDefault(peer, 0L)) {
                    matchIndex.put(peer, matched);
                    nextIndex.put(peer, matched + 1);
                }
            } else if ("STALE_TERM".equals(resp.reason())) {
                // 对端任期不应低于自己；记录即可
                log.debug("[{}] {} 以 STALE_TERM 拒绝（term={}）", nodeId, peer, resp.term());
            } else {
                long hint = resp.conflictIndex();
                long newNext;
                if (resp.conflictTerm() >= 0) {
                    // 在本节点日志中找该冲突任期的最后位置；找不到则回退到 hint
                    Long lastOfTerm = lastIndexOfTerm(resp.conflictTerm());
                    newNext = lastOfTerm != null ? lastOfTerm + 1 : Math.max(1, hint);
                } else {
                    newNext = Math.max(1, hint);
                }
                newNext = Math.min(newNext, ps.lastLogIndex() + 1);
                if (newNext < next) {
                    nextIndex.put(peer, newNext);
                    log.info("[{}] 与 {} 日志冲突，nextIndex {} -> {} 判定依据={} conflictTerm={}",
                            nodeId, peer, next, newNext, resp.reason(), resp.conflictTerm());
                }
            }
        } catch (ReplicatedLogException e) {
            // 分区或对端停机：下次心跳重试，不影响领导者身份
            log.debug("[{}] 复制到 {} 失败 reason={}", nodeId, peer, e.getReason());
        }
    }

    private Long lastIndexOfTerm(long targetTerm) {
        List<LogEntry> all = ps.log();
        for (int i = all.size() - 1; i >= 0; i--) {
            if (all.get(i).term() == targetTerm) {
                return (long) (i + 1);
            }
        }
        return null;
    }

    private void advanceLeaderCommit() {
        // 调用方持锁。Raft 安全规则：只提交当前任期的条目（N 提交后旧条目间接提交）。
        for (long n = ps.lastLogIndex(); n > ps.commitIndex(); n--) {
            LogEntry e = ps.entryAt(n);
            if (e.term() != term()) {
                continue; // 不直接提交之前任期的条目
            }
            int count = 1;
            for (String peer : peerIds) {
                if (matchIndex.getOrDefault(peer, 0L) >= n) {
                    count++;
                }
            }
            if (count >= quorum) {
                ps.saveCommitIndex(n);
                log.info("[{}] 领导者提升 commitIndex={} term={} 判定依据=当前任期条目获多数派 matchIndex 复制",
                        nodeId, n, term());
                break; // 日志连续，提升最高的即可
            }
        }
    }

    // ---------------- 客户端写入 ----------------

    /**
     * 写入受理规则：
     * <ol>
     *   <li>仅本节点为在任领导者可受理，否则 {@link RejectReason#NOT_LEADER}；</li>
     *   <li>领导者自己 + 可到达跟随者不足多数派时，{@link RejectReason#QUORUM_UNAVAILABLE}；</li>
     *   <li>条目持久化并复制到多数派、commitIndex 覆盖后才返回；等待超时为
     *       {@link RejectReason#REPLICATION_TIMEOUT}（分区恢复后重试同一 requestId 即可）；</li>
     *   <li>相同 requestId 永不重复入日志、不重复应用：命中已确认条目直接返回首次结果，
     *       并发重复提交共享同一轮复制。</li>
     * </ol>
     */
    public String submit(String requestId, String command) {
        Objects.requireNonNull(requestId, "requestId");
        Objects.requireNonNull(command, "command");
        lock.lock();
        try {
            if (!running) {
                throw new ReplicatedLogException(RejectReason.NODE_DOWN,
                        "节点 " + nodeId + " 已停机");
            }
            if (role != Role.LEADER) {
                throw new ReplicatedLogException(RejectReason.NOT_LEADER,
                        "节点 " + nodeId + " 不是领导者 role=" + role
                                + (leaderId != null ? " hint=" + leaderId : " 当前无领导者")
                                + " term=" + term());
            }

            // 幂等：已确认的重复提交
            Long existing = dedupeIndex.get(requestId);
            if (existing != null) {
                if (existing <= ps.commitIndex()) {
                    LogEntry committed = ps.entryAt(existing);
                    log.info("[{}] 幂等命中已确认提交 requestId={} index={} term={} 判定依据=请求标识已存在且已提交",
                            nodeId, requestId, existing, committed.term());
                    return committed.command();
                }
            }

            // 多数派可达性预判（自己算一票）
            int reachable = 1 + network.reachablePeers(nodeId).size();
            if (reachable < quorum) {
                log.warn("[{}] 拒绝写入 requestId={} term={} reachable={} quorum={} 判定依据=QUORUM_UNAVAILABLE",
                        nodeId, requestId, term(), reachable, quorum);
                throw new ReplicatedLogException(RejectReason.QUORUM_UNAVAILABLE,
                        "当前可联系节点 " + reachable + " 个，不足多数派 " + quorum
                                + "，分区期间不接受写入 term=" + term());
            }

            Object signal = pending.get(requestId);
            if (signal == null) {
                signal = new Object();
                pending.put(requestId, signal);
            }

            LogEntry entry;
            if (existing != null) {
                // 同一请求曾在旧的领导者任期写入但尚未确认，沿用原条目继续复制，保证不重复
                entry = ps.entryAt(existing);
                log.info("[{}] 继续复制未确认的既有条目 requestId={} index={} term={} 判定依据=幂等重试不新增日志",
                        nodeId, requestId, existing, entry.term());
            } else {
                long index = ps.lastLogIndex() + 1;
                entry = new LogEntry(term(), index, requestId, command);
                ps.append(List.of(entry));
                dedupeIndex.put(requestId, index);
                log.info("[{}] 受理写入 requestId={} index={} term={} command={}",
                        nodeId, requestId, index, term(), command);
            }

            long deadline = monotonicTick + submitTimeoutTicks;
            // 立即复制一轮；随后在锁上等待心跳/重传驱动提交
            broadcastAppendEntries();
            while (ps.commitIndex() < entry.index()) {
                if (!running) {
                    throw new ReplicatedLogException(RejectReason.NODE_DOWN,
                            "等待确认期间节点 " + nodeId + " 停机");
                }
                if (role != Role.LEADER) {
                    throw new ReplicatedLogException(RejectReason.NOT_LEADER,
                            "等待确认期间失去领导者身份 term=" + term());
                }
                try {
                    commitUpdated.await(50, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new ReplicatedLogException(RejectReason.REPLICATION_TIMEOUT,
                            "等待多数派确认被中断 requestId=" + requestId);
                }
                if (monotonicTick > deadline) {
                    log.warn("[{}] 写入确认超时 requestId={} index={} term={} commitIndex={} 判定依据=REPLICATION_TIMEOUT",
                            nodeId, requestId, entry.index(), term(), ps.commitIndex());
                    throw new ReplicatedLogException(RejectReason.REPLICATION_TIMEOUT,
                            "等待多数派确认超时 index=" + entry.index() + " commitIndex="
                                    + ps.commitIndex() + "（可用相同 requestId 重试）");
                }
            }
            pending.remove(requestId);
            advanceApplied();
            log.info("[{}] 写入已确认 requestId={} index={} term={} commitIndex={} lastApplied={} 判定依据=多数派复制完成",
                    nodeId, requestId, entry.index(), term(), ps.commitIndex(), sm.lastApplied());
            return entry.command();
        } finally {
            lock.unlock();
        }
    }

    /**
     * 法定人数读（ReadIndex 简化版）：领导者先向多数派发送一轮心跳确认自己仍为合法
     * 领导者，再返回状态机中不超过 commitIndex 的值。追随者直接以 NOT_LEADER 拒绝。
     */
    public String readQuorum(String key) {
        lock.lock();
        try {
            if (!running) {
                throw new ReplicatedLogException(RejectReason.NODE_DOWN,
                        "节点 " + nodeId + " 已停机");
            }
            if (role != Role.LEADER) {
                throw new ReplicatedLogException(RejectReason.NOT_LEADER,
                        "法定人数读必须经由领导者，节点 " + nodeId + " role=" + role
                                + (leaderId != null ? " hint=" + leaderId : " 当前无领导者"));
            }
            int ack = 1;
            int partitioned = 0;
            long readTerm = term();
            AppendEntriesRequest probe = new AppendEntriesRequest(
                    readTerm, nodeId, ps.lastLogIndex(), ps.lastLogTerm(), List.of(), ps.commitIndex());
            for (String peer : peerIds) {
                try {
                    AppendEntriesResponse resp = network.sendAppendEntries(nodeId, peer, probe);
                    if (resp.term() > readTerm) {
                        stepDown(resp.term());
                        throw new ReplicatedLogException(RejectReason.NOT_LEADER,
                                "读确认时发现更高任期 term=" + resp.term() + "，本节点已不是领导者");
                    }
                    ack++;
                } catch (ReplicatedLogException e) {
                    if (e.getReason() == RejectReason.NOT_LEADER) {
                        throw e;
                    }
                    partitioned++;
                }
            }
            if (ack < quorum) {
                throw new ReplicatedLogException(RejectReason.QUORUM_UNAVAILABLE,
                        "法定人数读无法确认领导者身份 ack=" + ack + " quorum=" + quorum
                                + " 不可达=" + partitioned);
            }
            advanceApplied();
            String value = sm.get(key);
            log.info("[{}] 法定人数读 key={} value={} term={} commitIndex={} ack={} 判定依据=多数派确认领导者身份",
                    nodeId, key, value, readTerm, ps.commitIndex(), ack);
            return value;
        } finally {
            lock.unlock();
        }
    }

    public String readLocal(String key) {
        lock.lock();
        try {
            return sm.get(key);
        } finally {
            lock.unlock();
        }
    }
}
