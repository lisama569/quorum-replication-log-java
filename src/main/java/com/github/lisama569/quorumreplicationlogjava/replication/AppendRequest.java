package com.github.lisama569.quorumreplicationlogjava.replication;

import java.util.List;

/**
 * 日志复制请求（心跳即 entries 为空的复制请求）。
 *
 * @param term         领导者任期
 * @param leaderId     领导者节点 ID
 * @param prevLogIndex 前导日志索引
 * @param prevLogTerm  前导日志任期
 * @param entries      待追加条目
 * @param leaderCommit 领导者已提交索引
 */
public record AppendRequest(long term, String leaderId, long prevLogIndex, long prevLogTerm,
                            List<LogEntry> entries, long leaderCommit) {
}
