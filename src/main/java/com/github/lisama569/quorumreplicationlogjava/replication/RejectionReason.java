package com.github.lisama569.quorumreplicationlogjava.replication;

/** 请求被拒绝的可区分原因。 */
public enum RejectionReason {
    /** 当前节点不是领导者。 */
    NOT_LEADER,
    /** 未达成多数派（分区或节点不足）。 */
    NO_QUORUM,
    /** 请求携带的任期过期。 */
    STALE_TERM,
    /** 日志在指定索引处冲突（前导条目不匹配）。 */
    LOG_CONFLICT,
    /** 请求的日志索引超出当前日志范围。 */
    INDEX_OUT_OF_RANGE,
    /** 集群暂时没有可确认的领导者（选举进行中，或领导者尚未在当前任期提交过条目）。 */
    NO_LEADER,
    /** 读前确认（心跳多数派）在等待时限内未完成。 */
    READ_CONFIRM_TIMEOUT
}
