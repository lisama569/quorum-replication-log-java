package com.github.lisama569.quorumreplicationlogjava.raft;

/**
 * RequestVote RPC 请求（Raft 论文 5.2 节）。
 */
public record RequestVoteRequest(
        long term,
        String candidateId,
        long lastLogIndex,
        long lastLogTerm
) {
}
