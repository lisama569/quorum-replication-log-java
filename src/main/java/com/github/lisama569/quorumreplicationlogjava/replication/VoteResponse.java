package com.github.lisama569.quorumreplicationlogjava.replication;

/**
 * 投票响应。
 *
 * @param term        响应者当前任期（用于候选人发现过期任期）
 * @param voteGranted 是否投票
 * @param reason      拒绝原因（同意时为 null）
 */
public record VoteResponse(long term, boolean voteGranted, RejectionReason reason) {
}
