package com.github.lisama569.quorumreplicationlogjava.replication;

/**
 * 复制日志条目。
 *
 * @param term      写入该条目时的领导者任期
 * @param index     日志索引（从 1 开始单调递增）
 * @param requestId 客户端请求标识，用于幂等去重
 * @param payload   业务数据
 */
public record LogEntry(long term, long index, String requestId, String payload) {
}
