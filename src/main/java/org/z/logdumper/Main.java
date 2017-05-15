package org.z.logdumper;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.regex.Pattern;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.StreamsConfig;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;

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
		SchemaRegistryClient schemaRegistry = new MockSchemaRegistryClient();
//		Executors.newSingleThreadExecutor().submit(() -> writeSomeData(schemaRegistry));
		
        try (KafkaConsumer<Object, Object> consumer = createKafkaConsumer(schemaRegistry);
        		RecordWriter writer = createRecordWriter();) {
	        List<ConsumerRecord<Object, Object>> recordsToDump = new ArrayList<>();
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
	
	private static KafkaConsumer<Object, Object> createKafkaConsumer(SchemaRegistryClient schemaRegistry) {
		Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "log-dumper");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put("group.id", "groupId");
        KafkaConsumer<Object, Object> consumer = new KafkaConsumer<>(props, 
        		new KafkaAvroDeserializer(schemaRegistry), new KafkaAvroDeserializer(schemaRegistry));
        consumer.subscribe(Pattern.compile(".*"), new ConsumerRebalanceListener() { //no-op consumer rebalance listener
			@Override
			public void onPartitionsRevoked(Collection<TopicPartition> partitions) {}
			
			@Override
			public void onPartitionsAssigned(Collection<TopicPartition> partitions) {}
		});
        return consumer;
	}
	
	private static RecordWriter createRecordWriter() {
		try {
			return new RecordWriter(new FileOutputStream("dump_file"));
		} catch (FileNotFoundException e) {
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
	private static void processRecords(ConsumerRecords<Object, Object> records, 
			List<ConsumerRecord<Object, Object>> recordsToDump, RecordWriter writer) {
		Instant dumpCutoffPoint = Instant.now().minus(DUMP_CUTOFF_POINT);
		List<ConsumerRecord<Object, Object>> recordsToDumpLater = new ArrayList<>();
		for (ConsumerRecord<Object, Object> record : records) {
    		if (record.timestamp() < dumpCutoffPoint.getEpochSecond()) {
    			recordsToDump.add(record);
    		} else {
    			recordsToDumpLater.add(record);
    		}
    	}
		dumpRecords(recordsToDump, recordsToDumpLater, writer);
	}

	private static void dumpRecords(List<ConsumerRecord<Object, Object>> recordsToDump, 
			List<ConsumerRecord<Object, Object>> recordsToDumpLater, RecordWriter writer) {
		recordsToDump.sort((record1, record2) -> Long.compare(record1.timestamp(), record2.timestamp()));
		writer.write(recordsToDump);
		recordsToDump.clear();
		recordsToDump.addAll(recordsToDumpLater);
	}
	
//	private static void writeSomeData(SchemaRegistryClient schemaRegistry) {
//		try{
//        	Thread.sleep(2000);
//        } catch (InterruptedException e) {
//        	
//        }
//		
//		Properties props = new Properties();
//        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "log-dumper");
//        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
//        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
//        props.put("group.id", "groupId");
//        KafkaProducer<Object, Object> producer = new KafkaProducer<>(props, 
//        		new KafkaAvroSerializer(schemaRegistry), new KafkaAvroSerializer(schemaRegistry));
//        
//        Schema schema = new Schema.Parser().parse("{\"type\": \"record\", \"name\": \"name\", \"fields\": [{\"name\": \"field\", \"type\": \"long\"}]}");
//        GenericRecord key1 = new GenericRecordBuilder(schema).set("field", 45L).build();
//        GenericRecord value1 = new GenericRecordBuilder(schema).set("field", 1234L).build();
//        producer.send(new ProducerRecord<Object, Object>("test", key1, value1));
//        
//        try{
//        	Thread.sleep(2000);
//        } catch (InterruptedException e) {
//        	
//        }
//        
//        GenericRecord key2 = new GenericRecordBuilder(schema).set("field", 56L).build();
//        GenericRecord value2 = new GenericRecordBuilder(schema).set("field", 1234L).build();
//        producer.send(new ProducerRecord<Object, Object>("test", key2, value2));
//        
//        try{
//        	Thread.sleep(2000);
//        } catch (InterruptedException e) {
//        	
//        }
//        
//        GenericRecord key3 = new GenericRecordBuilder(schema).set("field", 67L).build();
//        GenericRecord value3 = new GenericRecordBuilder(schema).set("field", 1234L).build();
//        producer.send(new ProducerRecord<Object, Object>("test", key3, value3));
//	}
}
