package org.z.logdumper.replay;

import java.io.File;
import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.z.logdumper.common.DumpFiles;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroSerializer;

public class Main {
	private static final String DUMP_DIRECTORY = System.getenv().getOrDefault("DUMP_DIRECTORY", ".");
	private static final String KAFKA_ADDRESS = System.getenv().getOrDefault("KAFKA_ADDRESS", "localhost:9092");
	
	public static void main(String[] args) throws InterruptedException {
		SchemaRegistryClient schemaRegistry = new MockSchemaRegistryClient();
		ExecutorService executor = Executors.newCachedThreadPool();
		
		System.out.println("initiating threads");
		initThreads(executor, schemaRegistry);
		executor.shutdown();
    	
		System.out.println("initiated threads, awaiting termination");
    	while(executor.awaitTermination(10, TimeUnit.MINUTES)) {
    		//zzzzz...
    	}
    	
    	System.out.println("Done");
	}
	
	private static void initThreads(ExecutorService executor, SchemaRegistryClient schemaRegistry) {
		File dumpDir = new File(DUMP_DIRECTORY);
		File[] dumpFiles = dumpDir.listFiles();
		for (File file : dumpFiles) {
			if (DumpFiles.isDumpFile(file)) {
				initThread(executor, schemaRegistry, file);
			}
		}
	}

	private static void initThread(ExecutorService executor, SchemaRegistryClient schemaRegistry, File file) {
		try {
			KafkaProducer<Object, Object> producer = createProducer(schemaRegistry);
			RecordReader reader = RecordReader.create(file);
			ReplayRecordConverter converter = createRecordConverter(file);
			executor.submit(() -> replayRecords(producer, reader, converter, file));
		} catch (IOException | RuntimeException e) {
			System.out.println("Failed to initialize reader for file " + file);
			e.printStackTrace();
		}
	}
	
	private static KafkaProducer<Object, Object> createProducer(SchemaRegistryClient schemaRegistry) {
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_ADDRESS);
		return new KafkaProducer<>(props, new KafkaAvroSerializer(schemaRegistry), 
				new KafkaAvroSerializer(schemaRegistry));
	}
	
	private static ReplayRecordConverter createRecordConverter(File file) {
		DumpFiles.TopicAndPartition topicAndPartition = DumpFiles.toTopicAndPartition(file);
		return new ReplayRecordConverter(topicAndPartition.topic, topicAndPartition.partition);
	}
	
	private static void replayRecords(KafkaProducer<Object, Object> producer, RecordReader reader, 
			ReplayRecordConverter converter, File file) {
		try {
			while(true) {
				GenericRecord replayRecord = reader.get();
				producer.send(converter.apply(replayRecord));
			}
		} catch (NoSuchElementException e) {
			System.out.println("Finished processing messages from file " + file);
		} catch (RuntimeException e) {
			System.out.println("Failed to process file " + file);
			e.printStackTrace();
		}
	}
}
