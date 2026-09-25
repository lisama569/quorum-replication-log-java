package com.github.lisama569.quorumreplicationlogjava.raft;

import java.util.List;

/**
 * AppendEntries RPC 请求（心跳时 entries 为空）。
 *
 * @param prevLogIndex 新条目紧邻之前的日志索引
 * @param prevLogTerm  prevLogIndex 对应条目的任期（prevLogIndex=0 时为 0）
 * @param entries      待追加的日志条目
 * @param leaderCommit 领导者已提交的最高索引
 */
public record AppendEntriesRequest(
        long term,
        String leaderId,
        long prevLogIndex,
        long prevLogTerm,
        List<LogEntry> entries,
        long leaderCommit
) {
}
