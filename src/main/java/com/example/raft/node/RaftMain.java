package com.example.raft.node;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RaftMain {
  private static final Logger logger = LoggerFactory.getLogger(RaftMain.class);
  private final int port;
  private Server grpcServer;

  public RaftMain(int port, RaftNode node) throws Exception {
    this.port = port;
    grpcServer = ServerBuilder.forPort(port).addService(node).build();
  }

  public void start() throws IOException {
    grpcServer.start();
    logger.info("Server started. Listening on port: {}", port);

    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  System.err.println("Shutting down grpc server since JVM is shutting down.");
                  try {
                    RaftMain.this.stop();
                  } catch (Exception e) {
                    e.printStackTrace(System.err);
                  }
                  System.err.println("Server shutdown complete");
                }));
  }

  public void stop() throws InterruptedException {
    if (grpcServer != null) {
      grpcServer.shutdown().awaitTermination(30, TimeUnit.SECONDS);
    }
  }

  // Await termination on the main thread since the gRPC library uses daemon threads
  public void blockUntilShutdown() throws InterruptedException {
    if (grpcServer != null) grpcServer.awaitTermination();
  }

  /**
   * We will start the cluster with 5 nodes. Each having the following properties Node1 - id:1 ;
   * port 9051 Node2 - id:2 ; port 9052 Node3 - id:3 ; port 9053 Node4 - id:4 ; port 9054 Node5 -
   * id:5 ; port 9055
   */
  public static void main(String[] args) throws Exception {
    if (args.length % 2 != 0) {
      logger.error("Even number of args required.");
      return;
    }

    int nodeId = Integer.parseInt(args[0]);
    int nodePort = Integer.parseInt(args[1]);
    Map<Integer, Integer> idToPortMap = new HashMap<>();
    for (int i = 2; i < args.length; i += 2) {
      idToPortMap.put(Integer.parseInt(args[i]), Integer.parseInt(args[i + 1]));
    }

    RaftNode node = new RaftNode(nodeId, new RaftNodePeers(idToPortMap));
    RaftMain raftServer = new RaftMain(nodePort, node);
    raftServer.start();
    raftServer.blockUntilShutdown();
  }
}
