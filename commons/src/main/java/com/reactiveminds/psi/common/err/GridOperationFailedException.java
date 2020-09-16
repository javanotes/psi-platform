package com.reactiveminds.psi.common.err;

import org.springframework.core.NestedRuntimeException;

public class GridOperationFailedException extends NestedRuntimeException {
    public GridOperationFailedException(String msg) {
        super(msg);
    }

    public GridOperationFailedException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
