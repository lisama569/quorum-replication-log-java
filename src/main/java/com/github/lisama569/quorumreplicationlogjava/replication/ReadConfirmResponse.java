package com.github.lisama569.quorumreplicationlogjava.replication;

/**
 * 读前确认响应。
 *
 * @param term         响应者当前任期（大于请求任期时表示领导者已过期）
 * @param acknowledged 是否认可该领导者在本任期的领导权
 */
public record ReadConfirmResponse(long term, boolean acknowledged) {
}
