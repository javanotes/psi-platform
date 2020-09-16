package com.reactiveminds.psi.common.kafka.tools;

import com.reactiveminds.psi.common.StopWatch;
import com.reactiveminds.psi.common.err.InternalOperationFailed;
import com.reactiveminds.psi.common.kafka.pool.BytesMessagePayload;
import com.reactiveminds.psi.common.kafka.pool.KafkaConsumerPool;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;


public class RequestReplyClient {

	@Autowired
	KafkaTemplate<byte[], byte[]> producer;
	@Autowired
	KafkaConsumerPool<byte[], byte[]> consumerPool;

	private byte[] awaitResult(RecordMetadata meta, Duration maxAwait, String correlationId) throws Exception {
		StopWatch timer = new StopWatch();
		timer.start();
		Consumer<byte[], byte[]> cons = consumerPool.acquire(5000, TimeUnit.MILLISECONDS, Collections.singletonMap(new TopicPartition(meta.topic(), meta.partition()), meta.offset()));
		try 
		{
			long maxAwaitMillis = maxAwait.toMillis();
			boolean matched = false;

			while(!matched) {
				if(timer.isExpired(maxAwaitMillis))
					throw new TimeoutException();

				ConsumerRecords<byte[], byte[]> records = cons.poll(Duration.ofMillis(1000));
				if(!records.isEmpty()) {
					timer.pause();
					for(ConsumerRecord<byte[], byte[]> rec: records) {
						Header hdr = null;
						if((hdr = rec.headers().lastHeader(KafkaHeaders.ACKNOWLEDGMENT)) != null){
							hdr = rec.headers().lastHeader(KafkaHeaders.CORRELATION_ID);
							String corrId = new String(hdr.value(), StandardCharsets.UTF_8);
							if(correlationId.equals(corrId)){
								return rec.value();
							}
						}
					}
					timer.resume();
				}

			}
			
		}
		finally {
			consumerPool.release(cons);
			timer.stop();
		}
		return null;
	}

	/**
	 *  @param message
	 * @param maxAwait
	 * @return
	 */
	public byte[] sendAndGet(BytesMessagePayload message, Duration maxAwait) {
		try{
			ProducerRecord<byte[], byte[]> producerRecord = new ProducerRecord<>(message.getRequestreplyTopic(), message.keyBytes(), message.getPayload());
			producerRecord.headers().add(KafkaHeaders.RAW_DATA, message.getKafkaStore().getBytes(StandardCharsets.UTF_8));
			producerRecord.headers().add(KafkaHeaders.CORRELATION_ID, message.getCorrelationId().getBytes(StandardCharsets.UTF_8));
			producerRecord.headers().add(KafkaHeaders.REPLY_TOPIC, message.getRequestreplyTopic().getBytes(StandardCharsets.UTF_8));
			RecordMetadata meta = producer.send(producerRecord).get().getRecordMetadata();
			return awaitResult(meta, maxAwait, message.getCorrelationId());

		} 
		catch (IOException e) {
			throw new InternalOperationFailed("Unable to sendget query", e);
		} 
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		} 
		catch (ExecutionException e) {
			throw new InternalOperationFailed("Unable to send query", e.getCause());
		}
		catch (TimeoutException e) {
			throw new InternalOperationFailed("Unable to get response - request timed out", e.getCause());
		}
		catch (Exception e) {
			throw new InternalOperationFailed("Unable to sendget query", e);
		}
		return null;
	}

}
