package com.example.raft.node;

import com.example.raft.proto.RaftService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VoteResponseHandler implements Consumer<RaftService.RequestVoteResponse> {

  private static final Logger logger = LoggerFactory.getLogger(VoteResponseHandler.class);

  private final AtomicInteger favourableVotesReceived = new AtomicInteger(0);
  private final RaftNode node;

  public VoteResponseHandler(RaftNode node) {
    this.node = node;
  }

  @Override
  public void accept(RaftService.RequestVoteResponse response) {
    logger.debug("Received response from client, for VoteRequest");
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
