package com.reactiveminds.psi.common.kafka.pool;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;

import javax.annotation.PostConstruct;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class KafkaConsumerPool<K, V> extends GenericObjectPool<PoolableConsumer<K, V>> {

	private static final Logger log = LoggerFactory.getLogger(KafkaConsumerPool.class);
	@Autowired
	private KafkaProperties kafkaProperties;
	public KafkaConsumerPool() {
		this(new ConsumerPoolFactory<K, V>());
	}
	@Value("${psi.consumer.maxPoolSize:10}")
	int maxPoolSize;
	@PostConstruct
	void init(){
		setConsumerProperties(kafkaProperties.buildConsumerProperties());
		setMaxTotal(maxPoolSize);
		setTestOnBorrow(true);
		setMaxWaitMillis(1000);
		setTimeBetweenEvictionRunsMillis(5000);
		log.info("Intialized consumer pool of- size -> {}, maxidle {}, waitms {}", getMaxTotal(), getMaxIdle(), getMaxWaitMillis());
	}

	/**
	 * Acquire consumer starting from a given offset
	 * @param maxwait
	 * @param unit
	 * @param topicPartitions
	 * @return
	 * @throws Exception
	 */
	public Consumer<K, V> acquire(long maxwait, TimeUnit unit, Map<TopicPartition, Long> topicPartitions) throws Exception {
		PoolableConsumer<K, V> consumer = borrowObject(unit.toMillis(maxwait));
		consumer.assign(topicPartitions.keySet());
		topicPartitions.keySet().forEach(tp -> {
			consumer.seek(tp, topicPartitions.get(tp));
		});
		return consumer;
	}

	/**
	 * Acquire consumer starting from beginning offset
	 * @param maxwait
	 * @param unit
	 * @param topicPartitions
	 * @return
	 * @throws Exception
	 */
	public Consumer<K, V> acquireLatest(long maxwait, TimeUnit unit, TopicPartition... topicPartitions) throws Exception {
		PoolableConsumer<K, V> consumer = borrowObject(unit.toMillis(maxwait));
		List<TopicPartition> partitionList = Arrays.asList(topicPartitions);
		consumer.assign(partitionList);
		consumer.seekToEnd(partitionList);
		return consumer;
	}
	public void release(Consumer<K, V> pooled) {
		if(!(pooled instanceof PoolableConsumer))
			throw new IllegalArgumentException("Not a type of PoolableConsumer!");
		returnObject((PoolableConsumer<K, V>) pooled);
	}
	private final ConsumerPoolFactory<K, V> poolFactory;
	private KafkaConsumerPool(ConsumerPoolFactory<K, V> thePool) {
		super(thePool);
		this.poolFactory = thePool;
	}
	public void setConsumerGroupPrefix(String prefix) {
		poolFactory.setConsumerGroupPrefix(prefix);
	}
	public void setConsumerProperty(String prop, Object val) {
		poolFactory.getConsumerProperties().put(prop, val);
	}
	public void setConsumerProperties(Map<? extends String, ? extends Object> props) {
		poolFactory.getConsumerProperties().putAll(props);
	}
}
