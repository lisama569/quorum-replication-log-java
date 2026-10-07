package com.github.lisama569.quorumreplicationlogjava.replication;

/**
 * 读前领导权确认响应。纯应答消息，处理它同样不产生任何状态变更。
 *
 * @param term  响应方当前任期（供发起方发现更高任期并退位）
 * @param grant 响应方是否认可请求中的领导者（请求任期不旧于本节点任期即认可）
 */
public record ReadConfirmResponse(long term, boolean grant) {
}
