package com.reactiveminds.psi.common.kafka.pool;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

class ConsumerProxy<K,V> implements InvocationHandler {

	private static final Logger log = LoggerFactory.getLogger(ConsumerProxy.class);
	private final KafkaConsumer<K, V> consumerInstance;
	private final KafkaConsumerPool<K, V> objectPoolInstance;
	private final boolean threadEnabled;
	
	public ConsumerProxy(Map<String, Object> configs, KafkaConsumerPool<K, V> objectPoolInstance) {
		consumerInstance = new KafkaConsumer<>(configs);
		this.objectPoolInstance = objectPoolInstance;
		if (objectPoolInstance.isHeartbeatThreadEnabled()) {
			timerThread = Executors.newSingleThreadScheduledExecutor(r -> {
				Thread t = new Thread(r, "ConsumerProxy-heartbeat-thread");
				t.setDaemon(true);
				return t;
			});
			threadEnabled = true;
		}
		else
			threadEnabled = false;

		log.info("** NEW PROXY INSTANCE - threadEnabled?{} **",threadEnabled);
	}

	private boolean subscriptionEnabled = false;
	private ScheduledExecutorService timerThread;
	private ScheduledFuture<?> future;
	@Override
	public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
		if(method.getName().equals("close") || method.getName().equals("unsubscribe")) {
			//skip methods
			return Void.TYPE;
		}
		if(!subscriptionEnabled && method.getName().equals("subscribe")) {
			throw new UnsupportedOperationException(" Possibly you do not need pooling for subscriber pattern. " +
					"Pools are really intended for short lived consumer actions");
		}
		if(method.getName().equals("destroyProxy")) {
			if(threadEnabled){
				if(wasPassivated)
					future.cancel(true);
				timerThread.shutdownNow();
			}
			consumerInstance.close();
			return Void.TYPE;
		}
		if(method.getName().equals("validateProxy")) {
			try {
				consumerInstance.listTopics(Duration.ofMillis(1000));
				return true;
			} catch (Exception e) {
				e.printStackTrace();
			}	
			return false;
		}
		if(method.getName().equals("activateProxy")) {
			if (wasPassivated) {
				if (threadEnabled) {
					future.cancel(true);
				}
				consumerInstance.resume(consumerInstance.paused());
				consumerInstance.unsubscribe();
				wasPassivated = false;
			}
			return Void.TYPE;
		}
		if(method.getName().equals("passivateProxy")) {
			consumerInstance.pause(consumerInstance.assignment());
			if (threadEnabled) {
				future = timerThread.scheduleWithFixedDelay(() -> consumerInstance.poll(Duration.ofMillis(100)), 1000, 1000, TimeUnit.MILLISECONDS);
			}
			wasPassivated = true;
			return Void.TYPE;
		}
		return method.invoke(consumerInstance, args);
	}

	private volatile boolean wasPassivated = false;

	public boolean isSubscriptionEnabled() {
		return subscriptionEnabled;
	}

	public void setSubscriptionEnabled(boolean subscriptionEnabled) {
		this.subscriptionEnabled = subscriptionEnabled;
	}

}
