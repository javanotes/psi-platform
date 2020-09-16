package com.reactiveminds.psi.streams.txn;

public interface TwoPCTransactionManager {
    /**
     * Create the transaction set and try become a coordinator node
     * @param id
     * @param map
     * @return
     */
    boolean tryElectCoordinator(String id, String map);

    void prepare(String txnId);
    void commit(String txnId);
    void rollback(String txnId);
}
