package com.github.lisama569.quorumreplicationlogjava.raft;

import java.util.List;

/**
 * 节点上必须在重启后仍然存在的状态（Raft 论文 Figure 2 中的 persistent state）。
 */
public interface PersistentState {

    long currentTerm();

    String votedFor();

    /** 保存当前任期，且同一原子动作内把投票目标重置（同任期重投由调用方保证不会发生）。 */
    void saveTermAndVote(long term, String votedFor);

    List<LogEntry> log();

    LogEntry entryAt(long index);

    long lastLogIndex();

    long lastLogTerm();

    /**
     * 按 Raft 规则追加日志：若与既有条目在同索引任期冲突则截断其后所有条目，
     * 已确认（已提交）的条目不允许被覆盖。
     */
    void append(List<LogEntry> newEntries);

    long commitIndex();

    void saveCommitIndex(long commitIndex);

    void close();
}
