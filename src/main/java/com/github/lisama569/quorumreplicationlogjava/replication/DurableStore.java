package com.github.lisama569.quorumreplicationlogjava.replication;

import java.util.List;

/**
 * 节点持久化状态抽象：重启后仍在的数据（任期、投票、日志）。
 * 进程内模拟时用 {@link InMemoryDurableStore}，重启节点时复用同一实例即可模拟磁盘保留。
 */
public interface DurableStore {

    long loadCurrentTerm();

    void saveCurrentTerm(long term);

    String loadVotedFor();

    void saveVotedFor(String votedFor);

    List<LogEntry> loadLog();

    void saveLog(List<LogEntry> log);
}
