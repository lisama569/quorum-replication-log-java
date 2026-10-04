package com.github.lisama569.quorumreplicationlogjava.replication;

/**
 * 读前领导者确认请求（ReadIndex 风格的心跳确认）。
 * 领导者在响应客户端读之前，用它向多数派确认自己仍是当前任期的有效领导者。
 *
 * @param term     领导者当前任期
 * @param leaderId 领导者节点 ID
 */
public record ReadConfirmRequest(long term, String leaderId) {
}
