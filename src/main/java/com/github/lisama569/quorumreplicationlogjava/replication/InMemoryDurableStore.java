package com.github.lisama569.quorumreplicationlogjava.replication;

import java.util.ArrayList;
import java.util.List;

/** 进程内持久化存储，模拟节点磁盘。 */
public class InMemoryDurableStore implements DurableStore {

    private long currentTerm;
    private String votedFor;
    private List<LogEntry> log = new ArrayList<>();

    @Override
    public synchronized long loadCurrentTerm() {
        return currentTerm;
    }

    @Override
    public synchronized void saveCurrentTerm(long term) {
        this.currentTerm = term;
    }

    @Override
    public synchronized String loadVotedFor() {
        return votedFor;
    }

    @Override
    public synchronized void saveVotedFor(String votedFor) {
        this.votedFor = votedFor;
    }

    @Override
    public synchronized List<LogEntry> loadLog() {
        return new ArrayList<>(log);
    }

    @Override
    public synchronized void saveLog(List<LogEntry> newLog) {
        this.log = new ArrayList<>(newLog);
    }
}
