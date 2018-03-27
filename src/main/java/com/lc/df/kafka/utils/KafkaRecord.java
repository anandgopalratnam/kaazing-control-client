package com.lc.df.kafka.utils;

public class KafkaRecord
{
	Object key;
	Object value;
	long offset;
	String topic;
	int partition;
	long timestamp;
	boolean inplay;

	public KafkaRecord(Object key,Object value) {
		this.key = key;
		this.value = value;
	}
	public Object getKey()
	{
		return key;
	}
	public void setKey(Object key)
	{
		this.key = key;
	}
	public Object getValue()
	{
		return value;
	}
	public void setValue(Object value)
	{
		this.value = value;
	}

	public long getOffset() {
		return offset;
	}

	public void setOffset(long offset) {
		this.offset = offset;
	}

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	public int getPartition() {
		return partition;
	}

	public void setPartition(int partition) {
		this.partition = partition;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	public boolean getInplay() {
		return inplay;
	}

	public void setInplay(boolean inplay) {
		this.inplay = inplay;
	}
}
