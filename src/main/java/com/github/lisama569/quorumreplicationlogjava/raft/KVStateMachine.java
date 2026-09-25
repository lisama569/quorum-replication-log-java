package com.github.lisama569.quorumreplicationlogjava.raft;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 简单 KV 状态机。
 *
 * <p>命令约定：{@code key=value} 写入；{@code GET:key} 读取（读取通常走 readQuorum，
 * 不会进入日志）；不满足上述格式的命令按序号保留在 {@code appliedCommands} 中。
 *
 * <p>同一 requestId 重复应用不生效（虽然 Raft 协议保证日志中每个 requestId 只出现一次，
 * 这里的去重是第二道防线，使状态机自身也具备幂等性）。
 */
public class KVStateMachine implements StateMachine {

    private static final Logger log = LoggerFactory.getLogger(KVStateMachine.class);

    private final Map<String, String> kv = new HashMap<>();
    private final Set<String> appliedRequestIds = new HashSet<>();
    private long lastApplied = 0;

    @Override
    public synchronized void apply(LogEntry entry) {
        if (!appliedRequestIds.add(entry.requestId())) {
            log.info("[SM] 幂等跳过 index={} term={} requestId={} 判定依据=该请求标识已应用",
                    entry.index(), entry.term(), entry.requestId());
            return;
        }
        lastApplied = entry.index();
        String command = entry.command();
        int eq = command.indexOf('=');
        if (eq > 0) {
            kv.put(command.substring(0, eq), command.substring(eq + 1));
        }
        log.info("[SM] 应用 index={} term={} requestId={} command={} lastApplied={} 判定依据=提交索引已覆盖该条目",
                entry.index(), entry.term(), entry.requestId(), command, lastApplied);
    }

    @Override
    public synchronized String get(String key) {
        return kv.get(key);
    }

    @Override
    public synchronized long lastApplied() {
        return lastApplied;
    }
}
