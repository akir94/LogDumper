package org.z.logdumper;

import java.io.FileOutputStream;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.streams.StreamsConfig;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;

/**
 * Polls kafka for records, and writes them in chronological order,
 * to make it easier to replay them later.
 * 
 * @author akir94
 *
 */
public class Main {
	private static final Duration POLL_INTERVAL = Duration.ofSeconds(1);
	private static final Duration DUMP_CUTOFF_POINT = Duration.ofMillis(250);
	
	public static void main(String[] args) {
		Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "log-dumper");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put("key.deserializer", new KafkaAvroDeserializer());
        props.put("value.deserializer", new KafkaAvroDeserializer());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try (KafkaConsumer<GenericRecord, GenericRecord> consumer = new KafkaConsumer<>(props);
        		RecordWriter writer = new RecordWriter(new FileOutputStream("dump_file"));) {
	        List<ConsumerRecord<GenericRecord, GenericRecord>> recordsToDump = new ArrayList<>();
	        while(true) {
	        	Instant start = Instant.now();
	        	processRecords(consumer.poll(0), recordsToDump, writer);
	        	Instant end = Instant.now();
	        	Duration sleepDuration = Duration.between(end, start.plus(POLL_INTERVAL));
	        	if (!sleepDuration.isNegative()) {
		        	try {
						Thread.sleep(sleepDuration.toMillis());
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
	        	}
	        }
        } catch (IOException e) {
			throw new ExceptionInInitializerError(e);
        }
	}
	
	/**
	 * It is important to note that it is possible to poll twice, and in the second poll
	 * receive a record that arrived earlier than a record we received in the previous poll.
	 * 
	 * To combat that, in every poll we split the records into two groups- those that arrived in the
	 * first part of the interval, and those that arrived in the second part.
	 * Those that arrived in the first part, we write immediately. Those that arrived in the second
	 * part, we put aside for the next poll.
	 * 
	 * @param records The records received in the current poll
	 * @param recordsToDump The records we put aside in the previous poll
	 * @param writer
	 */
	private static void processRecords(ConsumerRecords<GenericRecord, GenericRecord> records, 
			List<ConsumerRecord<GenericRecord, GenericRecord>> recordsToDump, RecordWriter writer) {
		Instant dumpCutoffPoint = Instant.now().minus(DUMP_CUTOFF_POINT);
		List<ConsumerRecord<GenericRecord, GenericRecord>> recordsToDumpLater = new ArrayList<>();
		for (ConsumerRecord<GenericRecord, GenericRecord> record : records) {
    		if (record.timestamp() < dumpCutoffPoint.getEpochSecond()) {
    			recordsToDump.add(record);
    		} else {
    			recordsToDumpLater.add(record);
    		}
    	}
		dumpRecords(recordsToDump, recordsToDumpLater, writer);
	}

	private static void dumpRecords(List<ConsumerRecord<GenericRecord, GenericRecord>> recordsToDump, 
			List<ConsumerRecord<GenericRecord, GenericRecord>> recordsToDumpLater, RecordWriter writer) {
		recordsToDump.sort((record1, record2) -> Long.compare(record1.timestamp(), record2.timestamp()));
		writer.write(recordsToDump);
		recordsToDump.clear();
		recordsToDump.addAll(recordsToDumpLater);
	}
}
