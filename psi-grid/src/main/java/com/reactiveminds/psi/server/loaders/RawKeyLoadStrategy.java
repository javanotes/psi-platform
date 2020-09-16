package com.reactiveminds.psi.server.loaders;

public interface RawKeyLoadStrategy {
    byte[] findByKey(byte[] key) throws Exception;
}
