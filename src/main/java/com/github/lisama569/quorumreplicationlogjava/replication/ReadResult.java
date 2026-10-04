package com.github.lisama569.quorumreplicationlogjava.replication;

import java.util.List;

/**
 * 一致性读结果。成功时给出读到的位置（任期 + 已提交索引）及对应条目；
 * 失败时给出可区分的原因，绝不退回本地未确认的数据。
 *
 * @param success   是否读到一致结果
 * @param term      读发生时的领导者任期（失败时为 -1）
 * @param readIndex 读到的已提交日志索引（失败时为 -1）
 * @param entries   截至 readIndex 的已提交条目（失败时为空表）
 * @param reason    失败原因（成功时为 null）
 * @param leaderId  实际服务该读的领导者 / 已知的领导者提示（可为 null）
 */
public record ReadResult(boolean success, long term, long readIndex, List<LogEntry> entries,
                         ReadFailureReason reason, String leaderId) {

    public static ReadResult ok(long term, long readIndex, List<LogEntry> entries, String leaderId) {
        return new ReadResult(true, term, readIndex, List.copyOf(entries), null, leaderId);
    }

    public static ReadResult failed(ReadFailureReason reason, String leaderId) {
        return new ReadResult(false, -1, -1, List.of(), reason, leaderId);
    }
}
