package com.example.raft.node;

public enum NodeState {
  FOLLOWER,
  CANDIDATE,
  LEADER;
}
