package com.github.lisama569.quorumreplicationlogjava.replication;

/**
 * 一致性读的读前确认请求（无副作用心跳）。
 *
 * <p>与 {@link AppendRequest} 的根本区别：不携带任何日志条目，也不携带
 * leaderCommit，接收方不得因此追加/截断日志或推进提交索引。它只用于
 * 领导者在放行读之前确认“自己在当前任期仍被多数派认可”。
 *
 * @param term     领导者任期
 * @param leaderId 领导者节点 ID
 */
public record ReadConfirmRequest(long term, String leaderId) {
}
