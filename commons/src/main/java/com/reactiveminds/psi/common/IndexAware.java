package com.reactiveminds.psi.common;

public interface IndexAware {
    long getWriteOffset();

    int getPartition();

    long getReadOffset();

    String getTopic();
}
