package com.github.lisama569.quorumreplicationlogjava.replication;

/**
 * 读前领导权确认请求（ReadIndex 流程专用）。
 *
 * <p>与 AppendRequest 严格分离：本消息<b>不携带任何日志条目、不携带
 * leaderCommit</b>，响应方处理它时不得追加/截断日志、不得推进提交索引、
 * 不得改变角色与已知领导者等任何状态。它只回答一个问题：
 * “我是否认可 term 任期的 leaderId 仍为合法领导者？”
 *
 * @param term     发起确认的领导者任期
 * @param leaderId 发起确认的领导者节点 ID
 */
public record ReadConfirmRequest(long term, String leaderId) {
}
