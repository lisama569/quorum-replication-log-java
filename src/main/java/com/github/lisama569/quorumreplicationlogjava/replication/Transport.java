package com.github.lisama569.quorumreplicationlogjava.replication;

import java.util.Optional;

/**
 * 节点间通信抽象。进程内集群用直接方法调用实现，
 * 分区通过丢弃/隔离消息来模拟。
 */
public interface Transport {

    /**
     * 向目标节点发送投票请求；目标不可达时返回空。
     */
    Optional<VoteResponse> sendVoteRequest(String targetId, VoteRequest request);

    /**
     * 向目标节点发送日志复制请求；目标不可达时返回空。
     */
    Optional<AppendResponse> sendAppendRequest(String targetId, AppendRequest request);

    /**
     * 向目标节点发送读前领导者确认请求；目标不可达时返回空。
     */
    Optional<ReadConfirmResponse> sendReadConfirm(String targetId, ReadConfirmRequest request);
}
