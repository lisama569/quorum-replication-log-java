package com.github.lisama569.quorumreplicationlogjava.raft;

/**
 * 操作被拒绝的可区分原因。
 */
public enum RejectReason {
    /** 集群尚未选出领导者。 */
    NO_LEADER,
    /** 请求被发往非领导者节点（可附带提示的领导者）。 */
    NOT_LEADER,
    /** 分区导致当前可用节点不足多数派，无法完成写入/法定人数读。 */
    QUORUM_UNAVAILABLE,
    /** 请求携带的任期早于节点当前任期。 */
    STALE_TERM,
    /** 追加位置的日志与领导者不匹配（索引冲突）。 */
    LOG_MISMATCH,
    /** 节点处于关闭/停机状态。 */
    NODE_DOWN,
    /** 网络分区，消息无法到达目标节点。 */
    NETWORK_PARTITION,
    /** 等待多数派确认超时。 */
    REPLICATION_TIMEOUT
}
