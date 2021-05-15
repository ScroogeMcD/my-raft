package com.example.raft.node;

import com.example.raft.proto.RaftProtocolNodeGrpc;
import com.example.raft.proto.RaftProtocolNodeGrpc.RaftProtocolNodeBlockingStub;
import com.example.raft.proto.RaftService.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RaftNodePeers {

  private static final Logger logger = LoggerFactory.getLogger(RaftNodePeers.class);

  /** Thread pool used by this thread to issue rpc calls to other servers in the cluster */
  private final ExecutorService rpcThreadPool = Executors.newFixedThreadPool(50);

  private final Map<Integer, RaftProtocolNodeBlockingStub> clientIdToStubMap;

  public RaftNodePeers(Map<Integer, Integer> peerIdToPortMap) {
    clientIdToStubMap = new HashMap<>();
    for (Map.Entry<Integer, Integer> entry : peerIdToPortMap.entrySet()) {
      clientIdToStubMap.put(entry.getKey(), createPeerStub(entry.getValue()));
    }
  }

  private RaftProtocolNodeBlockingStub createPeerStub(int port) {
    String target = "localhost:" + port;
    ManagedChannel channel = ManagedChannelBuilder.forTarget(target).usePlaintext().build();
    return RaftProtocolNodeGrpc.newBlockingStub(channel);
  }

  public void requestVoteFromPeers(
      RequestVoteParams requestVoteParams, Consumer<RequestVoteResponse> voteResponseHandler) {
    int currentTerm = requestVoteParams.getTerm();
    int candidateId = requestVoteParams.getCandidateId();
    logger.debug(
        "Starting vote request for candidateId: {}, termId: {} ", candidateId, currentTerm);

    // STEP 01 : Send RequestVote RPC to all peers in parallel
    List<Future<RequestVoteResponse>> voteResponseFutures = new ArrayList<>();

    for (Map.Entry<Integer, RaftProtocolNodeBlockingStub> entry : clientIdToStubMap.entrySet()) {
      var responseFuture =
          rpcThreadPool.submit(
              () -> {
                var blockingStub = entry.getValue();
                return blockingStub.requestVote(requestVoteParams);
              });
      voteResponseFutures.add(responseFuture);
    }

    // STEP 02 : check the response from the peers
    for (var future : voteResponseFutures) {
      RequestVoteResponse response = null;
      try {
        response = future.get(500, TimeUnit.MILLISECONDS);
      } catch (Exception e) {
        logger.warn("Exception occurred while requesting vote", e);
      }

      if (response != null) voteResponseHandler.accept(response);
    }
  }

  public void sendAppendEntriesToPeers(AppendEntriesParams params) {
    for (Map.Entry<Integer, RaftProtocolNodeBlockingStub> entry : clientIdToStubMap.entrySet()) {
      rpcThreadPool.submit(
          () -> {
            var blockingStub = entry.getValue();
            return blockingStub.appendEntries(params);
          });
    }
  }

  public Map<Integer, RaftProtocolNodeBlockingStub> getClientIdToStubMap() {
    return clientIdToStubMap;
  }
}
