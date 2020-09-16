package com.reactiveminds.psi.common.err;

import org.springframework.core.NestedRuntimeException;

public class LockWaitTimeoutException extends NestedRuntimeException {
    public LockWaitTimeoutException(String msg) {
        super(msg);
    }

    public LockWaitTimeoutException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
