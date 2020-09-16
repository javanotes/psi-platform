package com.reactiveminds.psi.streams.processor;

interface CommitProcessor {

    void commit(byte[] k, byte[] v);
}
