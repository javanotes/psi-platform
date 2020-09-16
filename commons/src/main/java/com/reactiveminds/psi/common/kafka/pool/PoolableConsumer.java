package com.reactiveminds.psi.common.kafka.pool;

import org.apache.kafka.clients.consumer.Consumer;

interface PoolableConsumer<K,V> extends Consumer<K, V> {

	void destroyProxy();
	boolean validateProxy();
	void activateProxy();
	void passivateProxy();
}
