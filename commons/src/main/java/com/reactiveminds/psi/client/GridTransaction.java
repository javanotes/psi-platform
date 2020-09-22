package com.reactiveminds.psi.client;

public interface GridTransaction {
    /**
     * Create a new transaction context. This context would be bound to the current thread.
     * @return
     */
    GridTransactionContext newTransaction();
}
