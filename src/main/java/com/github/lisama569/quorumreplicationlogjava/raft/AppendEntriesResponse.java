package com.github.lisama569.quorumreplicationlogjava.raft;

/**
 * AppendEntries RPC 响应。冲突时回传冲突点以加速回溯。
 */
public record AppendEntriesResponse(
        long term,
        boolean success,
        /** 成功时为追加后本节点的最新索引；失败时为建议领导者回退到的索引。 */
        long conflictIndex,
        /** 冲突位置处本节点条目的任期；无该条目时为 -1。 */
        long conflictTerm,
        String reason
) {
}
