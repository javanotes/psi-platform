package com.reactiveminds.psi.common.kafka.pool;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class BytesMessagePayload extends MessagePayload {

	public byte[] getPayload() {
		return payload;
	}

	public void setPayload(byte[] payload) {
		this.payload = payload;
	}

	public BytesMessagePayload(byte[] keyToQuery) {
		this.payload = keyToQuery;
	}

	public String getKafkaStore() {
		return kafkaStore;
	}

	public void setKafkaStore(String kafkaStore) {
		this.kafkaStore = kafkaStore;
	}

	private String kafkaStore;
	private byte[] payload;

	@Override
    public byte[] keyBytes() {
		return getCorrelationId().getBytes(StandardCharsets.UTF_8);
	}

	@Override
	protected byte[] serializePayload() throws IOException {
		return payload;
	}

	@Override
	protected void deserializePayload(byte[] b) throws IOException {

	}
}
