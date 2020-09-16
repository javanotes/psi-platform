package com.reactiveminds.psi.common.err;

import org.springframework.core.NestedRuntimeException;

public class WriteThroughOperationException extends NestedRuntimeException {
    public WriteThroughOperationException(String msg) {
        super(msg);
    }

    public WriteThroughOperationException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
