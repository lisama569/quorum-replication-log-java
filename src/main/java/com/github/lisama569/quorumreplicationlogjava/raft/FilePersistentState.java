package com.github.lisama569.quorumreplicationlogjava.raft;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 基于文件的持久化状态，模拟节点“重启不丢盘”。
 *
 * <p>两个文件：
 * <ul>
 *   <li>{@code state.dat}：当前任期、投票目标、已提交索引，每次变更走“临时文件 + 原子 move”，
 *       保证重启看到的要么是旧值要么是新值，不会出现半写。</li>
 *   <li>{@code log.dat}：日志条目序列，追加写；冲突截断后整体重写并原子替换。</li>
 * </ul>
 *
 * <p>冲突解决（Raft 论文 5.3/5.4）：如果新条目与既有条目同索引但任期不同，删除该索引及其后
 * 所有条目再追加；索引超过已提交索引的覆盖操作才被允许，已确认条目永不被静默丢弃。
 */
public class FilePersistentState implements PersistentState {

    private static final Logger log = LoggerFactory.getLogger(FilePersistentState.class);

    private final Path stateFile;
    private final Path logFile;
    private final String nodeId;

    private final Object lock = new Object();
    private long currentTerm;
    private String votedFor;
    private long commitIndex;
    private final List<LogEntry> entries = new ArrayList<>();

    public FilePersistentState(String dataDir, String nodeId) {
        Path dir = Path.of(dataDir, "node-" + nodeId);
        this.stateFile = dir.resolve("state.dat");
        this.logFile = dir.resolve("log.dat");
        this.nodeId = nodeId;
        load();
    }

    private void load() {
        synchronized (lock) {
            try {
                Files.createDirectories(stateFile.getParent());
                if (Files.exists(stateFile)) {
                    try (DataInputStream in = new DataInputStream(
                            new BufferedInputStream(Files.newInputStream(stateFile)))) {
                        currentTerm = in.readLong();
                        votedFor = in.readBoolean() ? in.readUTF() : null;
                        commitIndex = in.readLong();
                    }
                }
                if (Files.exists(logFile)) {
                    try (DataInputStream in = new DataInputStream(
                            new BufferedInputStream(Files.newInputStream(logFile)))) {
                        while (in.available() > 0) {
                            long term = in.readLong();
                            long index = in.readLong();
                            String requestId = in.readUTF();
                            String command = in.readUTF();
                            entries.add(new LogEntry(term, index, requestId, command));
                        }
                    }
                }
                log.info("[{}] 持久化恢复完成 term={} votedFor={} commitIndex={} lastLogIndex={}",
                        nodeId, currentTerm, votedFor, commitIndex, lastLogIndex());
            } catch (IOException e) {
                throw new UncheckedIOException("加载节点 " + nodeId + " 的持久化状态失败", e);
            }
        }
    }

    private void persistState() {
        synchronized (lock) {
            try {
                Path tmp = stateFile.resolveSibling("state.dat.tmp");
                try (DataOutputStream out = new DataOutputStream(Files.newOutputStream(tmp))) {
                    out.writeLong(currentTerm);
                    out.writeBoolean(votedFor != null);
                    if (votedFor != null) {
                        out.writeUTF(votedFor);
                    }
                    out.writeLong(commitIndex);
                }
                Files.move(tmp, stateFile, StandardCopyOption.REPLACE_EXISTING,
                        StandardCopyOption.ATOMIC_MOVE);
            } catch (IOException e) {
                throw new UncheckedIOException("持久化节点 " + nodeId + " 的状态失败", e);
            }
        }
    }

    private void persistLog() {
        synchronized (lock) {
            try {
                Path tmp = logFile.resolveSibling("log.dat.tmp");
                try (DataOutputStream out = new DataOutputStream(Files.newOutputStream(tmp))) {
                    for (LogEntry e : entries) {
                        out.writeLong(e.term());
                        out.writeLong(e.index());
                        out.writeUTF(e.requestId());
                        out.writeUTF(e.command());
                    }
                }
                Files.move(tmp, logFile, StandardCopyOption.REPLACE_EXISTING,
                        StandardCopyOption.ATOMIC_MOVE);
            } catch (IOException e) {
                throw new UncheckedIOException("持久化节点 " + nodeId + " 的日志失败", e);
            }
        }
    }

    @Override
    public long currentTerm() {
        synchronized (lock) {
            return currentTerm;
        }
    }

    @Override
    public String votedFor() {
        synchronized (lock) {
            return votedFor;
        }
    }

    @Override
    public void saveTermAndVote(long term, String vote) {
        synchronized (lock) {
            if (term < currentTerm) {
                throw new IllegalArgumentException(
                        nodeId + " 拒绝持久化过期任期 current=" + currentTerm + " given=" + term);
            }
            currentTerm = term;
            votedFor = vote;
            persistState();
            log.debug("[{}] 持久化任期 term={} votedFor={}", nodeId, term, vote);
        }
    }

    @Override
    public List<LogEntry> log() {
        synchronized (lock) {
            return List.copyOf(entries);
        }
    }

    @Override
    public LogEntry entryAt(long index) {
        synchronized (lock) {
            if (index < 1 || index > entries.size()) {
                return null;
            }
            return entries.get((int) (index - 1));
        }
    }

    @Override
    public long lastLogIndex() {
        synchronized (lock) {
            return entries.size();
        }
    }

    @Override
    public long lastLogTerm() {
        synchronized (lock) {
            return entries.isEmpty() ? 0 : entries.get(entries.size() - 1).term();
        }
    }

    @Override
    public void append(List<LogEntry> newEntries) {
        synchronized (lock) {
            for (LogEntry incoming : newEntries) {
                int pos = (int) (incoming.index() - 1);
                if (incoming.index() <= commitIndex) {
                    LogEntry committed = pos < entries.size() ? entries.get(pos) : null;
                    if (committed == null || committed.term() != incoming.term()
                            || !committed.requestId().equals(incoming.requestId())) {
                        throw new IllegalStateException(nodeId
                                + " 拒绝覆盖已确认条目 index=" + incoming.index()
                                + " commitIndex=" + commitIndex + " 判定依据=已确认日志不可变");
                    }
                    // 与已确认条目完全一致，幂等忽略。
                    continue;
                }
                if (pos < entries.size()) {
                    LogEntry existing = entries.get(pos);
                    if (existing.term() == incoming.term()
                            && existing.requestId().equals(incoming.requestId())) {
                        continue; // 同一条目的重传，幂等忽略
                    }
                    // 冲突：截断 pos 及其之后（这些索引都未提交）
                    entries.subList(pos, entries.size()).clear();
                    log.info("[{}] 日志冲突截断 fromIndex={} term {} -> {} 判定依据=同索引任期不一致",
                            nodeId, incoming.index(), existing.term(), incoming.term());
                }
                entries.add(incoming);
            }
            // 校验索引连续
            for (int i = 0; i < entries.size(); i++) {
                if (entries.get(i).index() != i + 1) {
                    throw new IllegalStateException(
                            nodeId + " 日志出现空洞 at=" + (i + 1));
                }
            }
            persistLog();
        }
    }

    @Override
    public long commitIndex() {
        synchronized (lock) {
            return commitIndex;
        }
    }

    @Override
    public void saveCommitIndex(long newCommitIndex) {
        synchronized (lock) {
            if (newCommitIndex < commitIndex) {
                throw new IllegalArgumentException(nodeId
                        + " commitIndex 不允许回退 current=" + commitIndex + " given=" + newCommitIndex);
            }
            if (newCommitIndex == commitIndex) {
                return;
            }
            if (newCommitIndex > entries.size()) {
                throw new IllegalArgumentException(nodeId
                        + " 不能提交不存在的索引 commitIndex=" + newCommitIndex
                        + " lastLogIndex=" + entries.size());
            }
            commitIndex = newCommitIndex;
            persistState();
        }
    }

    @Override
    public void close() {
        // 所有写操作都即时落盘，这里无需额外动作。
    }
}
