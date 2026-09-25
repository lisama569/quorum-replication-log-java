package com.github.lisama569.quorumreplicationlogjava.raft;

/**
 * 复制日志条目。
 *
 * @param term       条目被领导者接收时的任期
 * @param index      日志索引（从 1 开始，连续不空洞）
 * @param requestId  客户端请求标识，用于幂等去重
 * @param command    命令内容（演示中为 key:value 形式的字符串）
 */
public record LogEntry(long term, long index, String requestId, String command) {
}
