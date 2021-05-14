package com.example.raft.node;

import com.example.raft.proto.RaftProtocolNodeGrpc.*;
import com.example.raft.proto.RaftService.*;
import com.example.raft.rpc.client.RaftNodePeers;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

public class RaftNode extends RaftProtocolNodeImplBase {
  private static final Logger logger = Logger.getLogger(RaftNode.class.getName());
  /** fields representing the current node */
  private final int nodeId;

  private NodeState nodeState = NodeState.FOLLOWER;
  private AtomicInteger termNumber;
  private AtomicInteger candidateIdVotedFor;

  /** Peers of this node in the cluster */
  private RaftNodePeers peers;

  /** Fields around leader election timeout */
  private Random random = new Random();

  private ExecutorService leaderElectionThreadPool = Executors.newFixedThreadPool(10);
  private ScheduledExecutorService leaderHeartBeatThreadPool = Executors.newScheduledThreadPool(10);
  private long ELECTION_TIMEOUT_MILLIS;
  private final long LEADER_HEARTBEAT_INTERVAL_MILLIS = 50;
  private AtomicLong leaderElectionTime;

  private Path nodeStatePersistancePath;

  /** constructor */
  public RaftNode(int nodeId, RaftNodePeers peers) throws Exception {
    this.nodeId = nodeId;
    this.peers = peers;
    this.termNumber = new AtomicInteger(0);
    this.candidateIdVotedFor = new AtomicInteger(-1);

    nodeStatePersistancePath =
        Paths.get("/tmp/my-raft/", Integer.toString(nodeId), "raft-state.properties");
    ELECTION_TIMEOUT_MILLIS = random.nextInt(300 - 150) + 150;
    leaderElectionTime = new AtomicLong(System.currentTimeMillis() + ELECTION_TIMEOUT_MILLIS);
    // leaderElectionThreadPool.submit(leaderElectionTask);
    Thread thread = new Thread(leaderElectionTask);
    thread.start();
    readFromBackupState();
  }

  private final Runnable leaderElectionTask =
      () -> {
        while (true) {
          synchronized (this) {
            // logger.info("Inside while loop of leader election task");
            if (System.currentTimeMillis() >= leaderElectionTime.get()
                && nodeState != NodeState.LEADER) {
              beginElection();
              leaderElectionTime =
                  new AtomicLong(System.currentTimeMillis() + ELECTION_TIMEOUT_MILLIS);
            } else {
              try {
                // logger.info("Before wait");
                wait(ELECTION_TIMEOUT_MILLIS / 2);
                // logger.info("After wait");
              } catch (InterruptedException e) {
                e.printStackTrace();
              }
            }
          }
        }
      };

  private void beginElection() {
    logger.info("Leader election started.");
    // STEP 01: increment currentTerm and transition to CANDIDATE nodeState
    becomeCandidate();

    var requestVoteParams =
        RequestVoteParams.newBuilder().setTerm(termNumber.get()).setCandidateId(nodeId).build();
    peers.requestVoteFromPeers(requestVoteParams, new VoteResponseHandler(this));
  }

  private void becomeCandidate() {
    logger.info("Becoming candidate");
    termNumber.incrementAndGet();
    nodeState = NodeState.CANDIDATE;
  }

  public void stepDown(int termNumber) {
    logger.info("Stepping down. Higher term number received : " + termNumber);
    nodeState = NodeState.FOLLOWER;
    this.termNumber = new AtomicInteger(termNumber);
    this.candidateIdVotedFor = new AtomicInteger(-1);
  }

  public void becomeLeader() {
    logger.info("####### BECOMING LEADER");
    nodeState = NodeState.LEADER;
    leaderHeartBeatThreadPool.scheduleWithFixedDelay(
        () -> sendAppendEntriesToPeers(),
        0,
        LEADER_HEARTBEAT_INTERVAL_MILLIS,
        TimeUnit.MILLISECONDS);
  }

  private void sendAppendEntriesToPeers() {
    logger.info("Leader (id: " + nodeId + ") sending AppendEntries RPC to peers.");
    var appendEntriesParams =
        AppendEntriesParams.newBuilder().setTerm(termNumber.get()).setLeaderId(nodeId).build();
    peers.sendAppendEntriesToPeers(appendEntriesParams);
  }

  /**
   * This method allows the raft server to update the state on a local persistent storage, before
   * responding to RPCs
   */
  private void backupState() throws IOException {
    if (!Files.exists(nodeStatePersistancePath)) {
      Files.createFile(nodeStatePersistancePath);
    }

    // TODO 1: For now we are just overwriting the existing file. Eventually, all entries must be
    // appended.
    // TODO 2: also take care of node crashes in the midst of writing the state to file.
    // TODO 3: replace write by a buffered writer
    var nodePersistentState =
        PersistentState.RaftNodePersistentState.newBuilder()
            .setTerm(termNumber.get())
            .setCandidateIdVotedFor(candidateIdVotedFor.get())
            .build();
    byte[] stateToBePersisted = nodePersistentState.toByteArray();
    Files.write(nodeStatePersistancePath, stateToBePersisted);
  }

  private void readFromBackupState() throws Exception {
    if (Files.exists(nodeStatePersistancePath)) {
      byte[] stateReadFromStorage = Files.readAllBytes(nodeStatePersistancePath);
      var nodePersistentState =
          PersistentState.RaftNodePersistentState.parseFrom(stateReadFromStorage);
      termNumber = new AtomicInteger(nodePersistentState.getTerm());
      candidateIdVotedFor = new AtomicInteger(nodePersistentState.getCandidateIdVotedFor());
    }
  }

  @Override
  public void requestVote(
      RequestVoteParams request, StreamObserver<RequestVoteResponse> responseObserver) {
    var responseBuilder = RequestVoteResponse.newBuilder();
    var candidateTerm = request.getTerm();
    logger.info(
        "################# RV : CandidateId: "
            + request.getCandidateId()
            + ", MyTerm: "
            + termNumber.get()
            + ", candidateTerm: "
            + candidateTerm
            + ", candidateVotedFor: "
            + candidateIdVotedFor.get());

    if (candidateTerm < termNumber.get()) {
      var response = responseBuilder.setTerm(termNumber.get()).setVoteGranted(false).build();
      logger.info(
          "################# RV: vote declined for candidateId: " + request.getCandidateId());
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } else if (candidateIdVotedFor.get() == -1
        || candidateIdVotedFor.get() == request.getCandidateId()) {
      logger.info("************** RV: vote given for candidateId: " + request.getCandidateId());
      var response = responseBuilder.setTerm(termNumber.get()).setVoteGranted(true).build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
      candidateIdVotedFor = new AtomicInteger(request.getCandidateId());
    }
  }

  @Override
  public void appendEntries(
      AppendEntriesParams request, StreamObserver<AppendEntriesResponse> responseObserver) {
    var responseBuilder = AppendEntriesResponse.newBuilder();
    var requestTerm = request.getTerm();
    logger.info(
        "################# AE : Received heartbeat from leader. MyTerm:"
            + termNumber.get()
            + ", leader's term: "
            + requestTerm);
    if (requestTerm < termNumber.get()) {
      var response = responseBuilder.setTerm(termNumber.get()).setSuccess(false).build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } else {
      leaderElectionTime = new AtomicLong(System.currentTimeMillis() + ELECTION_TIMEOUT_MILLIS);
      var response = responseBuilder.setTerm(termNumber.get()).setSuccess(true).build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    }
  }

  public AtomicInteger getTermNumber() {
    return termNumber;
  }

  public NodeState getNodeState() {
    return nodeState;
  }

  public RaftNodePeers getPeers() {
    return peers;
  }
}
