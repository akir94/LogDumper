package org.z.logdumper.replay;

import java.util.function.Function;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

public class ReplayRecordConverter implements Function<GenericRecord, ProducerRecord<Object, Object>> {
	private String topic;
	private int partition;
	
	public ReplayRecordConverter(String topic, int partition) {
		this.topic = topic;
		this.partition = partition;
	}
	
	@Override
	public ProducerRecord<Object, Object> apply(GenericRecord replayRecord) {
		String key = (String) replayRecord.get("key");
		GenericRecord value = (GenericRecord) replayRecord.get("value");
		long timestamp = (long) replayRecord.get("timestamp");
		System.out.println(value.get("field"));
		return new ProducerRecord<Object, Object>(topic, partition, timestamp, key, value);
	}

}
