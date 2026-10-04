package com.github.lisama569.quorumreplicationlogjava.replication;

/**
 * 读前领导者确认响应。
 *
 * @param term         响应者当前任期（高于请求任期时领导者应退位）
 * @param confirmed    是否确认对方仍是有效领导者
 * @param commitIndex  响应者已知的已提交索引（用于领导者恢复提交进度）
 * @param lastLogIndex 响应者最后一条日志索引
 * @param reason       拒绝原因（确认时为 null）
 */
public record ReadConfirmResponse(long term, boolean confirmed, long commitIndex,
                                  long lastLogIndex, RejectionReason reason) {
}
