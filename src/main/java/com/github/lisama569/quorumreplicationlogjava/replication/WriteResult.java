package com.github.lisama569.quorumreplicationlogjava.replication;

/**
 * 写入结果。
 *
 * @param accepted 是否被受理（多数派确认）
 * @param index    已确认写入的日志索引（未受理时为 -1）
 * @param reason   拒绝原因（受理时为 null）
 * @param leaderId 当前已知领导者（用于客户端重定向，可为 null）
 */
public record WriteResult(boolean accepted, long index, RejectionReason reason, String leaderId) {

    public static WriteResult accepted(long index, String leaderId) {
        return new WriteResult(true, index, null, leaderId);
    }

    public static WriteResult rejected(RejectionReason reason, String leaderId) {
        return new WriteResult(false, -1, reason, leaderId);
    }
}
