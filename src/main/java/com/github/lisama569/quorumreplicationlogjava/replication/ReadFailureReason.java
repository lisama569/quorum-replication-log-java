package com.github.lisama569.quorumreplicationlogjava.replication;

/** 一致性读失败的可区分原因。 */
public enum ReadFailureReason {
    /** 集群中暂时没有可确认的领导者（如选举进行中、尚未选出领导者）。 */
    NO_LEADER,
    /** 当前节点不是领导者（leaderId 字段给出已知的领导者提示，可为空）。 */
    NOT_LEADER,
    /** 联系不上多数派，无法确认领导者仍然有效（如被隔离在少数派）。 */
    NO_QUORUM,
    /** 确认过程在超时时间内没有得到多数派回应。 */
    CONFIRM_TIMEOUT
}
