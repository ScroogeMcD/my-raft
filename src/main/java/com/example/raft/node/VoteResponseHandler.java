package com.example.raft.node;

import com.example.raft.proto.RaftService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.logging.Logger;

public class VoteResponseHandler implements Consumer<RaftService.RequestVoteResponse> {

  private static final Logger logger = Logger.getLogger(VoteResponseHandler.class.getName());

  private AtomicInteger favourableVotesReceived = new AtomicInteger(0);
  private RaftNode node;

  public VoteResponseHandler(RaftNode node) {
    this.node = node;
  }

  @Override
  public void accept(RaftService.RequestVoteResponse response) {
    // logger.info("Received response from client, for VoteRequest");
    int responseTermNumber = response.getTerm();
    boolean responseVote = response.getVoteGranted();
    if (node.getNodeState() == NodeState.CANDIDATE) {
      if (responseTermNumber >= node.getTermNumber().get()) {
        node.stepDown(responseTermNumber);
        return;
      }
      if (responseVote) {
        favourableVotesReceived.incrementAndGet();
        if (favourableVotesReceived.intValue()
            > node.getPeers().getClientIdToStubMap().size() / 2) {
          node.becomeLeader();
        }
      }
    }
  }
}
