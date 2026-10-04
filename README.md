# 多副本复制日志（进程内模拟集群）

在本仓库现有技术栈（Java 21 + Spring Boot + JUnit 5）内实现的 Raft 风格
多副本复制日志组件。集群由进程内节点模拟，可确定性复现**网络分区、节点
重启与并发写入**场景，并验证读写结论的一致性与可解释性。

## 模块结构

```
src/main/java/com/github/lisama569/quorumreplicationlogjava/replication/
├── RaftNode.java            节点核心：选举、日志复制、提交推进、幂等写入、一致性读
├── Cluster.java             进程内集群：消息路由、分区/重启/延迟模拟（实现 Transport）
├── Transport.java           节点间通信抽象（不可达 = Optional.empty）
├── DurableStore.java        持久化状态抽象（任期、投票、日志）
├── InMemoryDurableStore.java 进程内“磁盘”，重启节点时复用以模拟持久化
├── LogEntry.java            日志条目 (term, index, requestId, payload)
├── VoteRequest/VoteResponse   投票消息
├── AppendRequest/AppendResponse 复制（含心跳）消息
├── WriteResult.java         写入结果（受理/拒绝 + 可区分原因）
├── ReadResult.java          一致性读结果（读到的任期/日志位置 + 可区分失败原因）
├── NodeRole.java            FOLLOWER / CANDIDATE / LEADER
└── RejectionReason.java     NOT_LEADER / NO_QUORUM / STALE_TERM /
                             LOG_CONFLICT / INDEX_OUT_OF_RANGE /
                             NO_LEADER / READ_CONFIRM_TIMEOUT
```

## 一致性模型

- **单一领导者**：同一任期（term）内，多数派中每个节点最多投出一票，
  因此同一任期至多选出一个领导者；客户端写入仅由领导者受理。
- **多数派提交（quorum commit）**：写入只有在被复制到多数派后才推进
  提交索引（commitIndex）并对客户端确认；已确认的写入在后续任何
  分区/重启组合下都不会丢失。
- **已提交序列线性一致**：所有副本的已提交前缀完全一致；分区恢复后
  各副本收敛到同一日志序列（测试中以逐条相等断言验证）。
- **幂等写入**：每个客户端请求携带 `requestId`，重复提交返回首次写入
  的日志索引，不会被重复应用。

## 一致性读（客户端读已提交日志）

`RaftNode.clientRead`（集群入口 `Cluster.read(nodeId)`）提供面向客户端的
一致性读，语义为**线性一致读**：

- 读出的内容全部已经多数派确认（`entries` 即 `[1, readIndex]` 的已提交
  前缀，连续、无空洞、无重复）；
- 发起读之前已被确认（`WriteResult.accepted`）的写入，读的时候一定看得
  到——读的位置 `readIndex` 不小于任何先于它确认的写入索引；
- 读结果交代清楚读到的位置：`term`（确认本次读的领导者任期）与
  `readIndex`（已提交日志上界），便于排查与复现。

### 读的确认流程（ReadIndex 风格）

1. 仅领导者受理；非领导者以 `NOT_LEADER` 失败并附已知领导者提示；
2. 领导者须已在**当前任期**提交过条目，否则无法确认自己的提交索引
   是最新的，以 `NO_LEADER` 失败（新当选尚未有写入确认的窗口）；
3. 领导者向各副本发一轮心跳确认领导权仍然有效：凑齐多数派（含自己）
   才放行读；多数派确认在时限内完成后即可短路返回，不等慢副本；
4. 确认通过后以当前 `commitIndex` 为 `readIndex`，返回已提交条目快照。

### 读失败分类（彼此可区分，绝不回退为本地旧数据）

| 失败原因 | 含义 | 典型场景 |
|---|---|---|
| `NOT_LEADER` | 本节点不是领导者（附 `leaderId` 重定向提示，可为 null） | 集群未选举、选举进行中、节点已退位 |
| `NO_LEADER` | 暂时没有可确认的领导者 | 新领导者尚未提交本任期条目；确认中发现更高任期已退位 |
| `NO_QUORUM` | 心跳确认够不到多数派 | 领导者被隔离在少数派分区、多数副本宕机 |
| `READ_CONFIRM_TIMEOUT` | 多数派确认未在时限内完成 | 副本响应延迟超过读确认时限 |

被隔离的旧领导者仍自以为是 LEADER，但第 3 步的多数派确认必然失败，
因此它**不会**把本地旧数据当成最新结果返回；失败的读 `entries` 为空、
`readIndex = -1`。

本地验证方法：`Cluster.isolate(nodeId)` 模拟分区复现 `NO_QUORUM`；
`Cluster.setLatency(nodeId, ms)` 给副本响应注入延迟、配合
`Cluster.setReadConfirmTimeoutMillis(ms)` 复现 `READ_CONFIRM_TIMEOUT`；
不选举直接读或选举后隔离候选人复现 `NOT_LEADER`；新当选未写入时读
复现 `NO_LEADER`。以上均由 `ConsistentReadTest` 自动化覆盖。

### 适用范围与副作用

- **分区**：少数派一侧读失败（`NO_QUORUM`）；多数派一侧读正常。
- **重启**：重启后角色归零为 FOLLOWER，读以 `NOT_LEADER` 失败；重新
  当选并提交本任期条目后恢复可读。
- **选举窗口**：候选人/未确认领导者读失败（`NOT_LEADER`/`NO_LEADER`），
  不会返回未经验证的数据。
- **读写并发**：读不追加、不修改任何日志条目，不推进提交索引，不改变
  任期；失败重试无副作用。同一读者的连续读位置单调不回退。

## 选举与冲突解决规则

### 选举

1. 候选人 `term++`、投自己一票，**先持久化 (term, votedFor) 再发请求**，
   保证崩溃重启后同一任期不会重复投票；
2. 仅当候选人日志不比自己旧（比较 `(lastLogTerm, lastLogIndex)`）时才投票，
   保证当选者必然包含全部已提交条目；
3. 重复/重试的投票请求幂等：已投给该候选人则再次确认，已投给他人则拒绝；
4. 任何节点发现更高任期立即退位为 FOLLOWER 并更新任期——旧领导者无法
   继续以过期任期受理写入，杜绝双主。

### 日志复制与冲突

1. 复制请求携带 `(prevLogIndex, prevLogTerm)` 做前导一致性检查：
   - 前导索引超出本地日志 → 拒绝 `INDEX_OUT_OF_RANGE`；
   - 前导任期不匹配 → 拒绝 `LOG_CONFLICT`；
   - 请求任期过期 → 拒绝 `STALE_TERM`；
2. 领导者按每个副本的 `nextIndex` 逐条回退重试，直至找到匹配点；
3. 冲突条目**仅在未提交时才被截断**（`index > commitIndex`），已提交条目
   永不覆盖、永不静默丢弃（防御性检查：冲突落在已提交区间时直接拒绝）；
4. 领导者只提交**当前任期**内复制到多数派的条目（Raft 安全提交规则），
   旧任期条目随当前任期条目一起间接提交。

## 故障恢复语义

- **分区**：少数派一侧无法获得多数派确认，写入以 `NO_QUORUM` 拒绝，
  选举无法成功；多数派一侧继续正常服务。
- **分区恢复**：领导者通过心跳/复制使落后副本补齐日志并收敛到同一序列。
- **节点重启**：任期、投票、日志持久化在 `DurableStore`（模拟磁盘），
  重启后角色与提交索引等易失状态归零重建；跟随者由领导者心跳推进
  提交索引，日志完整的节点可重新当选领导者，已确认写入不丢失。
- **幂等重试**：相同 `requestId` 的重复提交（客户端超时重试等场景）
  返回首次的日志索引，各副本仅应用一次。

## 验证方法

```bash
mvn test
```

`QuorumReplicationTest` 覆盖 8 个场景，`ConsistentReadTest` 覆盖 5 个
一致性读场景，测试日志打印任期、日志索引/读位置与判定依据
（`[case]`/`[state]`/`[read]`/`[assert]` 前缀）：

| 测试 | 验证点 |
|---|---|
| `electionSingleLeaderPerTermAndDuplicateVotesSafe` | 同任期单领导者；重复投票幂等；过期任期拒绝 `STALE_TERM` |
| `minorityPartitionCannotElectLeaderOrAcceptWrites` | 少数派分区写入拒绝 `NO_QUORUM`、选举失败；多数派侧继续服务 |
| `partitionHealsAndReplicasConvergeWithoutLosingCommittedWrites` | 分区恢复后落后副本补齐日志，已提交序列逐条一致 |
| `conflictingUncommittedEntriesAreTruncatedCommittedOnesSurvive` | 未提交冲突条目确定性截断，已确认写入不丢失 |
| `staleTermAndIndexRequestsAreRejectedWithDistinctReasons` | `STALE_TERM` / `INDEX_OUT_OF_RANGE` / `LOG_CONFLICT` 可区分 |
| `restartedNodeCatchesUpAndCommittedWritesSurvive` | 跟随者/领导者重启后日志恢复、补齐提交索引、可重新当选 |
| `duplicateRequestIdIsAppliedOnlyOnce` | 相同 `requestId` 重试返回相同索引，仅应用一次 |
| `concurrentWritesGetUniqueOrderedIndexesAndConverge` | 8 线程并发写入索引唯一连续，全部副本收敛 |
| `writeThenReadSeesCommittedWriteAndReportsPosition` | 写后立读必见已确认写入；读结果报告 term/readIndex；读不改日志 |
| `isolatedOldLeaderCannotServeStaleReads` | 隔离旧领导者读失败 `NO_QUORUM` 不泄露旧数据；新领导者读到全部已确认写入；退位后 `NOT_LEADER` 附重定向 |
| `readFailureReasonsAreDistinctAndRetryHasNoSideEffects` | `NOT_LEADER`/`NO_QUORUM`/`READ_CONFIRM_TIMEOUT` 可区分；失败重试无副作用 |
| `readDuringElectionAndUnconfirmedLeaderWindow` | 选举进行中读失败 `NOT_LEADER`；未确认领导者读失败 `NO_LEADER`；确认后读成功 |
| `concurrentReadsAndWritesStayLinearizable` | 并发读写下读到的均为连续前缀、读位置单调不回退；读不产生额外日志条目 |

## 运行

```bash
mvn compile        # 编译
mvn test           # 运行全部测试
mvn spring-boot:run  # 启动 Spring Boot 应用（组件可独立嵌入使用）
```
