package com.reactiveminds.psi.common.err;

import org.springframework.core.NestedRuntimeException;

public class InternalOperationFailed extends NestedRuntimeException {
    public InternalOperationFailed(String msg) {
        super(msg);
    }

    public InternalOperationFailed(String msg, Throwable cause) {
        super(msg, cause);
    }
}
