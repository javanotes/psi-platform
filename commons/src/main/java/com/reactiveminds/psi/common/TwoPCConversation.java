package com.reactiveminds.psi.common;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public interface TwoPCConversation extends IndexAware{
    void begin(String hello);

    void tell(String hello);

    void send(String hello);

    String getCorrKey();

    String listen(long maxAwait, TimeUnit unit) throws TimeoutException;
}
