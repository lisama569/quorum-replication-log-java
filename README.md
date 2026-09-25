# Quorum Replication Log（进程内 Raft 多副本复制日志）

一套在本仓库现有技术栈（Java 21 + Spring Boot 3.x + JUnit 5）内**本地即可运行**的多副本
复制日志组件。它不依赖真实网络或多进程，而是在**单个 JVM 内用多个节点对象 + 可注入故障的
模拟网络**来还原 Raft 集群，使得「网络分区、节点重启、并发写入」下的读写结论**一致且可解释**：
每一条判定日志都会打印 **term（任期）、logIndex（日志索引）与判定依据**。

- 节点数：奇数（测试/演示使用 3 或 5），多数派 = `N/2 + 1`
- 调度：每个节点一个守护线程，按固定 tick（默认 10ms）驱动选举超时与心跳
- 网络：进程内同步 RPC，可注入对称分区，可模拟节点宕机/重启
- 持久化：每个节点一个数据目录，`state.dat`（任期/投票/提交索引）+ `log.dat`（日志），
  均以「临时文件 + 原子 rename」落盘，重启后恢复

## 目录结构

```
src/main/java/.../raft/
  Role.java                 节点角色 FOLLOWER/CANDIDATE/LEADER
  RejectReason.java         所有拒绝原因的可区分枚举
  ReplicatedLogException    携带 RejectReason 的异常
  LogEntry.java             日志条目 (term, index, requestId, command)
  RequestVote*/AppendEntries*  RPC 请求/响应记录类型
  PersistentState.java      持久化状态接口
  FilePersistentState.java  基于文件的持久化 + 冲突截断
  StateMachine.java/KVStateMachine.java  已提交日志的 KV 应用（幂等）
  Network.java              进程内网络：分区注入、宕机判定、同步投递
  RaftNode.java             选举、投票、复制、提交、客户端读写的核心
  RaftCluster.java          集群装配、调度器、故障注入与读写入口
src/main/java/.../demo/
  QuorumDemo.java           不依赖 Web 容器的本地演示入口
src/test/java/.../tests/    分区 / 选举安全 / 重启 / 冲突 / 并发 / 幂等等测试
```

## 一、一致性模型

- **写（强一致 / 线性一致）**：只有在任领导者能受理写入；条目被复制到**多数派**并把
  `commitIndex` 推进覆盖该索引后，`submit` 才返回成功。因此「返回成功的写入」一定已在
  多数派持久化，任何后续的多数派读都必然能读到（已确认写入不会丢、不会读旧值）。
- **读（法定人数读 ReadIndex 的简化实现）**：`readQuorum` 先由领导者向多数派发送一轮
  心跳确认自己仍是合法领导者（若对端返回更高任期则立即退位），再从已应用到
  `commitIndex` 的状态机读取。这保证读不到过期领导者的数据。
  `readLocal` 只读本节点已应用状态，**可能陈旧**，仅用于观察副本收敛过程。
- **日志匹配（Log Matching）**：连续索引 + 每条目带任期；AppendEntries 通过
  `prevLogIndex/prevLogTerm` 一致性检查保证不同节点日志一旦在某索引匹配，其之前的所有
  条目也相同，从而使各副本最终收敛到**同一条日志序列**。
- 客户端**用 requestId 实现幂等**：相同 requestId 的重复/并发提交只会产生一条日志、
  应用一次，重复提交直接返回首次结果。

## 二、选举规则（为什么同一任期绝不会有两个主）

1. **一任期一票**：节点把 `(currentTerm, votedFor)` 持久化。同一任期内已经投过票，就只
   能把票再次投给**同一候选人**（RPC 重试安全、结果幂等），改投其他候选人会被
   `ALREADY_VOTED:<id>` 拒绝。
2. **多数派当选**：候选人必须获得多数派选票才能成为领导者。两个互相分区的多数派集合
   不可能同时存在，因此**同一任期最多一个领导者**。
3. **候选人日志足够新才得票**：比较 `(lastLogTerm, lastLogIndex)`，候选人的日志至少与
   投票者一样新才可能拿到票（`LOG_BEHIND`），防止日志落后者当选覆盖已提交数据。
4. **随机选举超时**：每个节点在 `[minElection, maxElection]` tick 区间随机取超时，
   降低多候选人分票概率；未达多数派时等待下一次超时重试，重复投票/重试不会产生双主。
5. **任期单调 + 见到更高任期即退位**：任何 RPC 请求/响应携带的任期高于本节点时，节点
   立即退回 FOLLOWER 并更新任期；**过期任期的请求被明确拒绝，原因 `STALE_TERM`**。
6. 新领导者当选后立即写入一条**当前任期 no-op** 并复制到多数派，使之前任期的已复制
   条目随之被安全提交（规避论文 Figure 8 的跨任期提交歧义）。

## 三、写入受理与提交规则

| 情形 | 结论 | RejectReason |
| --- | --- | --- |
| 向非领导者提交 | 拒绝，异常信息附带当前领导者 hint | `NOT_LEADER` |
| 当前无领导者（选举中） | 拒绝 | `NO_LEADER` |
| 领导者在分区中、可联系节点不足多数派 | 拒绝（入口预检 + 确认等待双重保护） | `QUORUM_UNAVAILABLE` |
| 等待多数派确认超时 | 拒绝，可用同一 requestId 重试 | `REPLICATION_TIMEOUT` |
| 请求携带过期任期 | 拒绝 | `STALE_TERM` |
| 跟随者缺少 prevLogIndex | 拒绝，返回回退建议索引 | `LOG_MISMATCH:PREV_INDEX_MISSING` |
| prevLogIndex 存在但任期不一致 | 拒绝，返回冲突任期与回退索引 | `LOG_MISMATCH:PREV_TERM` |
| 目标节点已停机 | 网络层/节点直接拒绝 | `NODE_DOWN` |
| 源/目标被分区 | 网络层拒绝（与停机可区分） | `NETWORK_PARTITION` |

提交规则遵循 Raft 安全约束：领导者**只直接提交当前任期**的条目（由多数派 matchIndex
统计），之前任期的条目在当前任期条目（no-op）被提交后随日志匹配性质一并提交。

## 四、冲突解决规则（确定，不静默丢已确认条目）

跟随者处理 AppendEntries 时：

1. `prevLogIndex` 超过本地末尾：返回 `PREV_INDEX_MISSING`，建议回退到 `本地末尾+1`。
2. `prevLogIndex` 处任期不一致：返回该位置的本地任期（`conflictTerm`）与该任期在本地
   的**首条索引 - 1**；领导者据此把 `nextIndex` 快速回退到冲突任期的最后一条之后
   （找不到该任期则回退到对端建议索引）。
3. 真正写入时，若新条目与既有条目**同索引但 (term 或 requestId) 不一致**：
   **删除该索引及其之后的所有条目，再追加**。
4. 关键安全边界：索引 `<= commitIndex` 的是**已确认条目，不可变**。任何试图覆盖已确认
   条目的操作都会抛出异常（`拒绝覆盖已确认条目`），冲突截断只能发生在未提交区段，
   因此**已确认写入永不丢失，且冲突处理是确定的、可观察的（日志打印截断点与新旧任期）**。

## 五、故障恢复语义

- **网络分区**：分区期间只有含多数派的一侧能选出领导者并提交；少数派一侧的旧领导者
  无法凑齐多数派，所有写入在入口即被 `QUORUM_UNAVAILABLE` 拒绝。恢复后，旧领导者收到
  更高任期心跳后退位，并按上面的冲突回溯规则截断未提交分叉、补齐缺失日志，最终与
  多数派日志完全一致。
- **节点宕机（stopNode）**：停止 tick 调度，丢弃易失状态（角色、状态机内存）。
  对该节点的消息以 `NODE_DOWN` 失败（与 `NETWORK_PARTITION` 区分）。磁盘文件保留。
- **节点重启（restartNode）**：重新创建节点，从 `state.dat/log.dat` 恢复
  任期、votedFor、commitIndex 与全部日志，并重建 requestId 幂等表；空状态机从索引 1
  重放已提交日志。随后由领导者心跳通过快速回溯把落后日志补齐。
- **未确认写入的幂等重试**：若某次写入在等待确认时超时/失败，其条目可能只停留在少数
  节点。客户端用**同一个 requestId 重试**：领导者识别到该请求已在日志中则继续复制原条目，
  绝不重复入日志、不重复应用。
- **并发写入**：多个客户端线程并发提交时，由领导者在节点锁内串行定序，索引连续不空洞；
  每个 requestId 在所有副本上的 (index, term) 完全一致。

> 进程内并发安全说明：节点所有状态变更都在单把 `ReentrantLock` 内串行完成；RPC 处理入口
> 使用 `tryLock`，拿不到锁立刻返回 BUSY（由调用方下一轮重试），从而避免「持 A 锁同步
> 调用 B、同时持 B 锁调用 A」的跨节点锁环。

## 六、验证方法

### 运行测试

```bash
mvn test
```

17 个测试（均为不绑定端口的纯 JUnit 用例，数据目录用 `@TempDir`）覆盖：

| 测试类 | 覆盖内容 |
| --- | --- |
| `SmokeElectionTest` | 唯一领导者选举、多数派写后法定人数读可见、三副本收敛 |
| `ElectionSafetyTest` | 同任期一票、重复投票幂等、改投被拒、3-vs-2 分区不同时出双主 |
| `PartitionTest` | 少数派写入 `QUORUM_UNAVAILABLE`、多数派重选、`STALE_TERM`、恢复收敛 |
| `RejectReasonTest` | NOT_LEADER / QUORUM_UNAVAILABLE / NO_LEADER / NODE_DOWN / STALE_TERM / 两种 LOG_MISMATCH |
| `RestartTest` | 跟随者宕机重启补齐、领导者宕机重选、重启后已确认写入不丢 |
| `ConflictResolutionTest` | 未提交分叉被确定性截断、已确认区段逐索引不变 |
| `IdempotencyTest` | 串行/并发同 requestId 只入一条日志、不同 requestId 同命令各自成条 |
| `ConcurrentWriteTest` | 12 线程并发写日志逐索引一致、写后法定人数读不读旧值 |

测试运行时在控制台/`target/surefire-reports` 中可看到形如：

```
[n3] 当选领导者 term=2 ... 判定依据=同一任期多数派投票
[n2] 拒绝 AppendEntries 来自 n1 termReq=1 termLocal=2 判定依据=STALE_TERM 过期任期
[n1] 日志冲突截断 fromIndex=5 term 1 -> 2 判定依据=同索引任期不一致
[n3] 写入已确认 requestId=req-1 index=2 term=2 commitIndex=2 判定依据=多数派复制完成
[SM] 幂等跳过 index=... 判定依据=该请求标识已应用
```

### 运行本地演示（观察完整故障过程）

```bash
mvn compile exec:java
```

演示按顺序执行：正常写入 → 隔离旧领导者（写入被 QUORUM_UNAVAILABLE 拒绝）→
多数派更高任期重选并写入 → 网络恢复全集群收敛 → 节点宕机/重启补齐，
最后打印 5 个节点完全一致的 term/索引/状态机值。

### Spring Boot 应用

`mvn spring-boot:run` 仍可正常启动（`server.port=0` 随机端口）。Raft 组件本身不依赖
Web 容器，演示与测试均可独立运行。

## 七、范围说明（教学/演示性质）

这是一个用于解释「法定人数复制一致性」的进程内参考实现，刻意简化了真实生产 Raft 的
部分工程细节：网络是同步方法调用而非真实 IO、不做日志快照（snapshot）与日志压缩、
成员变更固定、客户端写请求不做集群内转发（只返回领导者 hint）。核心的选举安全、
多数派提交、冲突回溯、已确认不丢与幂等语义均按 Raft 论文（2014 扩展版）实现并有测试证明。
