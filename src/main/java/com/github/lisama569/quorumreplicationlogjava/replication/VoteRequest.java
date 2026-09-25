package com.github.lisama569.quorumreplicationlogjava.replication;

/**
 * 投票请求。
 *
 * @param term         候选人任期
 * @param candidateId  候选人节点 ID
 * @param lastLogIndex 候选人最后一条日志索引
 * @param lastLogTerm  候选人最后一条日志任期
 */
public record VoteRequest(long term, String candidateId, long lastLogIndex, long lastLogTerm) {
}
