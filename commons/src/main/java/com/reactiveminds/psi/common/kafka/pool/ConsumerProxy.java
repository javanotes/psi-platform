package com.reactiveminds.psi.common.kafka.pool;

import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

class ConsumerProxy<K,V> implements InvocationHandler {

	private final KafkaConsumer<K, V> instance;
	
	public ConsumerProxy(Map<String, Object> configs) {
		instance = new KafkaConsumer<>(configs);
		timerThread = Executors.newSingleThreadScheduledExecutor(r -> {
			Thread t = new Thread(r, "ConsumerProxy-heartbeat-thread");
			t.setDaemon(true);
			return t;
		});
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
			instance.close();
			return Void.TYPE;
		}
		if(method.getName().equals("validateProxy")) {
			try {
				instance.listTopics(Duration.ofMillis(1000));
				return true;
			} catch (Exception e) {
				e.printStackTrace();
			}	
			return false;
		}
		if(method.getName().equals("activateProxy")) {
			if (wasPassivated) {
				future.cancel(true);
				instance.resume(instance.paused());
				instance.unsubscribe();
				wasPassivated = false;
			}
			return Void.TYPE;
		}
		if(method.getName().equals("passivateProxy")) {
			instance.pause(instance.assignment());
			future = timerThread.scheduleWithFixedDelay(() -> instance.poll(Duration.ofMillis(100)), 1000, 1000, TimeUnit.MILLISECONDS);
			wasPassivated = true;
			return Void.TYPE;
		}
		return method.invoke(instance, args);
	}

	private volatile boolean wasPassivated = false;

	public boolean isSubscriptionEnabled() {
		return subscriptionEnabled;
	}

	public void setSubscriptionEnabled(boolean subscriptionEnabled) {
		this.subscriptionEnabled = subscriptionEnabled;
	}

}
