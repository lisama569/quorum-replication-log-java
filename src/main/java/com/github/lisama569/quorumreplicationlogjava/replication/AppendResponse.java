package com.github.lisama569.quorumreplicationlogjava.replication;

/**
 * 日志复制响应。
 *
 * @param term    响应者当前任期
 * @param success 是否接受
 * @param reason  拒绝原因（成功时为 null）
 * @param matchIndex 响应者当前与领导者匹配的最大日志索引
 */
public record AppendResponse(long term, boolean success, RejectionReason reason, long matchIndex) {
}
