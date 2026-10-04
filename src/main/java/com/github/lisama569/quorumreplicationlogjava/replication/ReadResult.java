package com.github.lisama569.quorumreplicationlogjava.replication;

import java.util.List;

/**
 * 一致性读结果。
 *
 * <p>成功的读附带“读到的位置”：确认读时的领导者任期 {@code term} 与
 * 已提交日志上界 {@code readIndex}，{@code entries} 为 [1, readIndex]
 * 区间内的已提交条目，便于排查与复现。
 *
 * @param success  是否读成功（失败时不得回退为本地未确认数据）
 * @param term     确认本次读的领导者任期（失败时为 -1）
 * @param readIndex 读到的已提交日志上界（失败时为 -1）
 * @param entries  已提交条目快照（失败时为空列表）
 * @param reason   失败原因（成功时为 null），彼此可区分
 * @param leaderId 服务本次读的领导者 / 重定向提示（可为 null）
 */
public record ReadResult(boolean success, long term, long readIndex, List<LogEntry> entries,
                         RejectionReason reason, String leaderId) {

    public static ReadResult ok(long term, long readIndex, List<LogEntry> entries, String leaderId) {
        return new ReadResult(true, term, readIndex, List.copyOf(entries), null, leaderId);
    }

    public static ReadResult failed(RejectionReason reason, String leaderId) {
        return new ReadResult(false, -1, -1, List.of(), reason, leaderId);
    }
}
