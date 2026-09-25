package com.github.lisama569.quorumreplicationlogjava.raft;

/**
 * RequestVote RPC 响应。
 */
public record RequestVoteResponse(
        long term,
        boolean voteGranted,
        String reason
) {
}
