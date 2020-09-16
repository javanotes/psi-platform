package com.reactiveminds.psi.common.err;

import org.springframework.core.NestedRuntimeException;

public class ReadThroughOperationException extends NestedRuntimeException {
    public ReadThroughOperationException(String msg) {
        super(msg);
    }

    public ReadThroughOperationException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
