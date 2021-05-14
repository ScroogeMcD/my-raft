package com.example.raft.node;

import org.junit.Test;

public class RaftNodeTest {

  @Test
  public void statePersistanceTest() throws Exception {
    /*RaftNode raftNode = RaftNode.getInstance();

    int updatedTerm = 11;
    int updatedVotedFor = 13;

    PersistentState.RaftNodePersistentState updatedState =
        PersistentState.RaftNodePersistentState.newBuilder()
            .setTerm(updatedTerm)
            .setCandidateIdVotedFor(updatedVotedFor)
            .build();
    raftNode.setNodePersistentState(updatedState);

    raftNode.backupState();
    raftNode.readFromBackupState();

    Assert.assertEquals(updatedTerm, raftNode.getNodePersistentState().getTerm());
    Assert.assertEquals(
        updatedVotedFor, raftNode.getNodePersistentState().getCandidateIdVotedFor());*/
  }
}
