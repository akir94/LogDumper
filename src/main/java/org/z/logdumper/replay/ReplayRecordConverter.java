package org.z.logdumper.replay;

import java.util.function.Function;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * Converts GenericRecord from dump files to ProducerRecord that can be sent to kafka.
 * 
 * Note- doesn't copy the timestamp from the GenericRecord to the ProducerRecord,
 * in case some consumer of the messages compares the timestamps to the current time
 * (and thus might experience weird behaviour).
 * 
 * @author akir94
 *
 */
public class ReplayRecordConverter implements Function<GenericRecord, ProducerRecord<Object, Object>> {
	private String topic;
	private int partition;
	
	public ReplayRecordConverter(String topic, int partition) {
		this.topic = topic;
		this.partition = partition;
	}
	
	@Override
	public ProducerRecord<Object, Object> apply(GenericRecord replayRecord) {
		Utf8 key = (Utf8) replayRecord.get("key");
		GenericRecord value = (GenericRecord) replayRecord.get("value");
		return new ProducerRecord<Object, Object>(topic, partition, key, value);
	}

}
