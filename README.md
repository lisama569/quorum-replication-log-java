# 多副本复制日志（进程内模拟集群）

在本仓库现有技术栈（Java 21 + Spring Boot + JUnit 5）内实现的 Raft 风格
多副本复制日志组件。集群由进程内节点模拟，可确定性复现**网络分区、节点
重启与并发写入**场景，并验证读写结论的一致性与可解释性。

## 模块结构

```
src/main/java/com/github/lisama569/quorumreplicationlogjava/replication/
├── RaftNode.java            节点核心：选举、日志复制、提交推进、幂等写入、零副作用一致性读
├── Cluster.java             进程内集群：消息路由、分区/重启/延迟模拟（实现 Transport）
├── Transport.java           节点间通信抽象（不可达 = Optional.empty）
├── DurableStore.java        持久化状态抽象（任期、投票、日志）
├── InMemoryDurableStore.java 进程内“磁盘”，重启节点时复用以模拟持久化
├── LogEntry.java            日志条目 (term, index, requestId, payload)
├── VoteRequest/VoteResponse   投票消息
├── AppendRequest/AppendResponse 复制（含心跳）消息
├── ReadConfirmRequest/ReadConfirmResponse 读前领导权确认（**纯查询、零副作用**）消息
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

### 读零副作用（重要不变量）

**读，无论成功还是失败、无论重试多少次，都不改变任何节点的任何复制状态：**

- 不追加、不截断、不覆盖任何日志条目（条目内容与条目数量不变）；
- 不推进任何节点（含领导者与跟随者）的已提交位置 `commitIndex`；
- 不改变任期、角色、`votedFor`、`knownLeaderId` 与领导者的 `nextIndex`；
- 不触发任何落盘；读失败后重试，重试前后各节点状态逐字节一致。

因此：落后副本分区恢复后**单凭一次读不会被“补齐”**——它只能通过真正的
复制流程（写入触发的 `replicate` / 心跳）追赶；客户端当时收到失败/未确认
结果的写入（只暂存在旧领导者本地的未提交条目）也**绝不会因为一次读**出现
在其他副本的日志里。推进日志与提交位置是写/复制的唯一职责。

实现上，读的领导权确认走**独立的只读 RPC**（`ReadConfirmRequest` /
`ReadConfirmResponse`），与复制消息 `AppendRequest` 物理分离：确认消息
不带任何日志条目、不带 `leaderCommit`；响应方 `handleReadConfirm` 只读
任期并应答，不执行任何赋值。这样即使未来复制消息演进，读路径也不可能
重新捎带上日志或提交位置。

### 读的确认流程（ReadIndex 风格）

1. 仅领导者受理；非领导者以 `NOT_LEADER` 失败并附已知领导者提示；
2. 领导者须已在**当前任期**提交过条目，否则无法确认自己的提交索引
   是最新的，以 `NO_LEADER` 失败（新当选尚未有写入确认的窗口）；
3. 领导者向各副本发一轮**只读确认**验证领导权仍然有效：自己一票，
   凑齐多数派认可才放行读；确认在时限内凑齐多数派即可短路返回，不等
   慢副本。该轮消息纯查询，对发送方与响应方都零副作用；
4. 确认通过后以**发起确认时记录的** `commitIndex` 为 `readIndex`，
   返回已提交条目快照（`[1, readIndex]`，连续、无空洞、无重复）。

### 读失败分类（彼此可区分，绝不回退为本地旧数据）

| 失败原因 | 含义 | 典型场景 |
|---|---|---|
| `NOT_LEADER` | 本节点不是领导者（附 `leaderId` 重定向提示，可为 null） | 集群未选举、选举进行中（候选人）、节点已退位 |
| `NO_LEADER` | 暂时没有可确认的领导者 | 新领导者尚未提交本任期条目；确认中发现更高任期已退位 |
| `NO_QUORUM` | 只读确认凑不齐多数派认可 | 领导者被隔离在少数派分区、多数副本宕机（不可达即时判定，不等超时） |
| `READ_CONFIRM_TIMEOUT` | 多数派确认越过时限才凑齐（或未凑齐） | 副本响应延迟超过读确认时限；与 `NO_QUORUM` 明确区分 |

被隔离的旧领导者仍自以为是 LEADER，但第 3 步的多数派只读确认必然失败，
因此它**不会**把本地旧数据当成最新结果返回；失败的读 `entries` 为空、
`readIndex = -1`。四类失败都不改动任何节点状态。

本地验证方法：`Cluster.isolate(nodeId)` 模拟分区复现 `NO_QUORUM`；
`Cluster.setLatency(nodeId, ms)` 给副本响应注入延迟、配合
`Cluster.setReadConfirmTimeoutMillis(ms)` 复现 `READ_CONFIRM_TIMEOUT`；
不选举直接读复现 `NOT_LEADER`；新当选未写入时读复现 `NO_LEADER`。
验证“读零副作用”：在读/失败读前后对全部节点快照
（term/role/commitIndex/日志内容与条数）并断言逐字节一致——
`ReadNoSideEffectTest`、`ReadFailureZeroResidueTest` 已自动化覆盖。

### 适用范围与副作用

- **分区**：少数派一侧读失败（`NO_QUORUM`）；多数派一侧读正常。落后副本
  恢复连通后，读不会顺带补齐它的日志或提交位置——追赶只发生在真正的
  复制流程中（下一次写入/显式心跳）；在那之前它可以作为确认方响应读，
  但自身状态纹丝不动。
- **重启**：重启后角色归零为 FOLLOWER，读以 `NOT_LEADER` 失败；重新
  当选并提交本任期条目后恢复可读。
- **选举窗口**：候选人读失败（`NOT_LEADER`）；新当选但本任期尚无已提交
  条目的领导者读失败（`NO_LEADER`）；确认中发现更高任期时领导者本地
  退位并以 `NO_LEADER` 失败——不会返回未经验证的数据。
- **读写并发**：读到的永远是 `[1, readIndex]` 连续前缀，无空洞、无重复、
  无错位；同一读者连续成功读的位置单调不回退；读不返回比已确认内容更旧
  的值（读索引取多数派确认时刻的领导者 `commitIndex`）。读不追加/修改
  任何日志条目，不推进任何节点提交索引，不改变任期；失败重试无副作用，
  重试前后各节点状态完全一致，不留任何残留。

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

### 手工复现“读带副作用”的旧缺陷

修复前读确认复用了复制消息（`AppendRequest`），会把领导者日志尾部与
`leaderCommit` 捎给响应方。可用下面的最小序列手工复现（5 节点）：

1. n1 当选，提交 v1，`replicate` 同步基线；隔离 n4/n5 后再提交 v2、v3；
2. `heal` n4/n5，**不调用 `replicate`**，直接 `read("n1")`；
3. 旧实现：n4/n5 日志凭空多出 v2/v3、`commitIndex` 前跳；
   修复后：n4/n5 仍是 1 条日志、`commitIndex=1`，再调 `replicate` 才收敛。

同类复现：隔离 n1 后发起一个必然失败（`NO_QUORUM`）的写入，恢复后
只发读——旧实现会把客户端已收到失败的条目复制到其他副本，修复后不会。

`QuorumReplicationTest` 覆盖 8 个场景，`ConsistentReadTest` 覆盖 5 个
一致性读场景，`ReadNoSideEffectTest` / `ReadFailureZeroResidueTest` /
`ConcurrentReadNoSideEffectTest` 覆盖 7 个“读零副作用”回归场景，测试
日志打印任期、日志索引/读位置与判定依据
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
| `readDoesNotReplicateToReconnectedLaggingReplica` | **重点**：落后副本恢复连通后只读不复制，其日志/提交位置冻结；只有 `replicate()` 才追赶 |
| `rejectedWriteIsNotSpreadByRead` | **重点**：客户端收到失败（`NO_QUORUM`）的写入，连续读后仍不出现在其他副本，全员提交位置不变 |
| `noQuorumReadFailuresAndRetriesLeaveNoResidue` | 隔离领导者多次失败读、落后副本恢复后重试：全员状态零残留；成功读同样不捎带日志 |
| `timeoutReadFailuresAndRetriesLeaveNoResidue` | 确认超时两次失败读零状态变更；延迟清除后重试成功且仍零副作用 |
| `noLeaderWindowFailuresLeaveNoResidue` | 无领导者 `NOT_LEADER`、新当选未确认 `NO_LEADER`，失败不回退本地旧数据、零残留 |
| `readsDuringElectionWindowFailCleanlyAndNeverServeOldData` | 选举进行中候选人 `NOT_LEADER`、旧领导者 `NO_QUORUM`；新任期提交后读到含旧确认值的连续前缀 |
| `concurrentReadsNeverReplicateAndStayLinearizable` | 并发读写+副本分区抖动；确定性追赶窗口内读风暴不得给落后副本补日志/提提交位置，只有复制能收敛 |

## 运行

```bash
mvn compile        # 编译
mvn test           # 运行全部测试
mvn spring-boot:run  # 启动 Spring Boot 应用（组件可独立嵌入使用）
```
