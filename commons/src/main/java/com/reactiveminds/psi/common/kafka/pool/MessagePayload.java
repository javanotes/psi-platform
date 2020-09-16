package com.reactiveminds.psi.common.kafka.pool;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public abstract class MessagePayload implements Externalizable{

	public enum MessageType{REQUEST, RESPONSE}
	/**
	 * Return a serialized value of the key
	 * @return
	 */
	protected abstract byte[] keyBytes();
	/**
	 * Return a serialized value of the payload T
	 * @return
	 * @throws IOException
	 */
	protected abstract byte[] serializePayload() throws IOException;
	/**
	 * Deserialize to an instance of payload T. Subclasses should use this method to
	 * set the deserialized value to the payload instance
	 * @param b
	 * @throws IOException
	 */
	protected abstract void deserializePayload(byte[] b) throws IOException;
	
	public String getCorrelationId() {
		return correlationId;
	}
	public void setCorrelationId(String correlationId) {
		this.correlationId = correlationId;
	}
	
	private MessageType type = MessageType.REQUEST;
	private String correlationId;
	private String requestreplyTopic;
	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		out.writeUTF(correlationId);
		out.writeUTF(requestreplyTopic);
		out.writeUTF(type.name());
		byte[] b = serializePayload();
		out.writeInt(b.length);
		out.write(b);
	}
	@Override
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
		readHeaders(in);
		byte[] b = new byte[in.readInt()];
		in.read(b);
		deserializePayload(b);
	}
	void readHeaders(ObjectInput in) throws IOException, ClassNotFoundException {
		correlationId = in.readUTF();
		requestreplyTopic = in.readUTF();
		type = MessageType.valueOf(in.readUTF());
	}
	public String getRequestreplyTopic() {
		return requestreplyTopic;
	}
	public void setRequestreplyTopic(String requestreplyTopic) {
		this.requestreplyTopic = requestreplyTopic;
	}
	public MessageType getType() {
		return type;
	}
	public void setType(MessageType type) {
		this.type = type;
	}
}
