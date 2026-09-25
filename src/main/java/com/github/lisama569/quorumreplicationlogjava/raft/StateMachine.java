package com.github.lisama569.quorumreplicationlogjava.raft;

/**
 * 应用已提交日志条目的状态机。
 * 命令约定 "key=value" 表示写入，"GET:key" 表示读取；其余命令原样按序号记录。
 */
public interface StateMachine {

    /** 幂等地应用一条已提交条目（重复 requestId 不再生效）。 */
    void apply(LogEntry entry);

    String get(String key);

    /** 已应用到的日志索引。 */
    long lastApplied();
}
