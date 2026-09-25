package com.github.lisama569.quorumreplicationlogjava.raft;

/**
 * 复制日志组件对外抛出的带可区分原因的异常。
 */
public class ReplicatedLogException extends RuntimeException {

    private final RejectReason reason;

    public ReplicatedLogException(RejectReason reason, String message) {
        super(message);
        this.reason = reason;
    }

    public RejectReason getReason() {
        return reason;
    }
}
