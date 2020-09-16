package com.reactiveminds.psi.common.err;

public class GridOperationNotAllowedException extends IllegalArgumentException {
    public GridOperationNotAllowedException(String s) {
        super(s);
    }

    public GridOperationNotAllowedException(String message, Throwable cause) {
        super(message, cause);
    }
}
