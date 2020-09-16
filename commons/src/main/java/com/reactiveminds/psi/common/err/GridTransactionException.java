package com.reactiveminds.psi.common.err;

import org.springframework.core.NestedRuntimeException;

public class GridTransactionException extends NestedRuntimeException {
    public GridTransactionException(String msg) {
        super(msg);
    }

    public GridTransactionException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
