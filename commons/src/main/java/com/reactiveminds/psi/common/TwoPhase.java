package com.reactiveminds.psi.common;

public interface TwoPhase {
    String PREPARE = "PREPARE";
    String PREPARE_ACK = "PREPARE_ACK";
    String PREPARE_NACK = "PREPARE_NACK";
    String COMMIT = "COMMIT";
    String COMMIT_ACK = "COMMIT_ACK";
    String COMMIT_NACK = "COMMIT_NACK";
    String END = "END";
    String END_ACK = "END_ACK";
    String END_NACK = "END_NACK";
    String ABORT = "ABORT";
}
