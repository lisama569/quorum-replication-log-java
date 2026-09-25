# 多副本复制日志（进程内模拟集群）

在本仓库现有技术栈（Java 21 + Spring Boot + JUnit 5）内实现的 Raft 风格
多副本复制日志组件。集群由进程内节点模拟，可确定性复现**网络分区、节点
重启与并发写入**场景，并验证读写结论的一致性与可解释性。

## 模块结构

```
src/main/java/com/github/lisama569/quorumreplicationlogjava/replication/
├── RaftNode.java            节点核心：选举、日志复制、提交推进、幂等写入
├── Cluster.java             进程内集群：消息路由、分区/重启模拟（实现 Transport）
├── Transport.java           节点间通信抽象（不可达 = Optional.empty）
├── DurableStore.java        持久化状态抽象（任期、投票、日志）
├── InMemoryDurableStore.java 进程内“磁盘”，重启节点时复用以模拟持久化
├── LogEntry.java            日志条目 (term, index, requestId, payload)
├── VoteRequest/VoteResponse   投票消息
├── AppendRequest/AppendResponse 复制（含心跳）消息
├── WriteResult.java         写入结果（受理/拒绝 + 可区分原因）
├── NodeRole.java            FOLLOWER / CANDIDATE / LEADER
└── RejectionReason.java     NOT_LEADER / NO_QUORUM / STALE_TERM /
                             LOG_CONFLICT / INDEX_OUT_OF_RANGE
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

`QuorumReplicationTest` 覆盖 8 个场景，测试日志打印任期、日志索引与
判定依据（`[case]`/`[state]`/`[assert]` 前缀）：

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

## 运行

```bash
mvn compile        # 编译
mvn test           # 运行全部测试
mvn spring-boot:run  # 启动 Spring Boot 应用（组件可独立嵌入使用）
```
