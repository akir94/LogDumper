package org.z.logdumper.dump;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.regex.Pattern;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.util.Utf8;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.StreamsConfig;

import akka.actor.ActorSystem;
import akka.kafka.ConsumerSettings;
import akka.kafka.Subscriptions;
import akka.kafka.javadsl.Consumer;
import akka.stream.ActorMaterializer;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;

/**
 * Listens to kafka messages, and writes them to files, 
 * with a separate file for each topic and partition.
 * 
 * @author akir94
 *
 */
public class Main {
	private static final String DUMP_DIRECTORY = System.getenv("DUMP_DIRECTORY");
	private static final String KAFKA_ADDRESS = System.getenv().getOrDefault("KAFKA_ADDRESS", "localhost:9092");
	
	public static void main(String[] args) throws InterruptedException {
		SchemaRegistryClient schemaRegistry = new MockSchemaRegistryClient();
		ActorSystem system = ActorSystem.create();
		ActorMaterializer materializer = ActorMaterializer.create(system);

//		Executors.newSingleThreadExecutor().submit(() -> writeSomeData(schemaRegistry));
//		Thread.sleep(2200);
		
    	Map<String, List<PartitionInfo>> topics = getAllTopicsAndPartitions();
    	for(Map.Entry<String, List<PartitionInfo>> entry : topics.entrySet()) {
    		for (PartitionInfo info : entry.getValue()) {
    			RecordWriter writer = RecordWriter.create(DUMP_DIRECTORY, info.topic(), info.partition());
    			createSource(system, schemaRegistry, info.topic(), info.partition())
    				.to(Sink.foreach(r -> writer.accept(r)))
    				.run(materializer);
    		}
    	}
    	
    	Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				materializer.shutdown();
				system.terminate();
			}
		});
    	
    	while (true) {
    		Thread.sleep(10000);
    	}
	}
	
	private static Map<String, List<PartitionInfo>> getAllTopicsAndPartitions() {
		KafkaConsumer<Object, Object> consumer = createKafkaConsumer();
		subscribeConsumerToAllTopics(consumer);
		return consumer.listTopics();
	}

	private static KafkaConsumer<Object, Object> createKafkaConsumer() {
		Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_ADDRESS);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "groupId");
        return new KafkaConsumer<>(props, 
        		new KafkaAvroDeserializer(), new KafkaAvroDeserializer());
	}
	
	private static void subscribeConsumerToAllTopics(KafkaConsumer<Object, Object> consumer) {
		consumer.subscribe(Pattern.compile(".*"), new ConsumerRebalanceListener() { //no-op consumer rebalance listener
			@Override
			public void onPartitionsRevoked(Collection<TopicPartition> partitions) {}
			
			@Override
			public void onPartitionsAssigned(Collection<TopicPartition> partitions) {}
		});
	}
	
	private static Source<ConsumerRecord<Object, Object>, Consumer.Control> createSource(ActorSystem system, 
			SchemaRegistryClient schemaRegistry, String topic, int partition) {
		ConsumerSettings<Object, Object> settings = 
				ConsumerSettings.create(system, new KafkaAvroDeserializer(schemaRegistry), new KafkaAvroDeserializer(schemaRegistry))
			        .withBootstrapServers(KAFKA_ADDRESS)
			        .withGroupId("group2")
			        .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		return Consumer.plainSource(settings, 
				Subscriptions.assignmentWithOffset(new TopicPartition(topic, partition), 0));
	}
	
//	private static void writeSomeData(SchemaRegistryClient schemaRegistry) {
//		Properties props = new Properties();
//        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "log-dumper");
//        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_ADDRESS);
//        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
//        props.put("group.id", "groupId");
//        KafkaProducer<Object, Object> producer = new KafkaProducer<>(props, 
//        		new KafkaAvroSerializer(schemaRegistry), new KafkaAvroSerializer(schemaRegistry));
//        
//        Schema schema = SchemaBuilder.builder().record("record").fields()
//        		.name("field").type().longType().noDefault()
//        		.endRecord();
//        
//        Utf8 key1 = new Utf8("key1");
//        GenericRecord value1 = new GenericRecordBuilder(schema).set("field", 1234L).build();
//        producer.send(new ProducerRecord<Object, Object>("test", key1, value1));
//        try{
//        	Thread.sleep(2000);
//        } catch (InterruptedException e) {
//        	
//        }
//        
//        Utf8 key2 = new Utf8("key2");
//        GenericRecord value2 = new GenericRecordBuilder(schema).set("field", 1234L).build();
//        producer.send(new ProducerRecord<Object, Object>("test", key2, value2));
//        try{
//        	Thread.sleep(2000);
//        } catch (InterruptedException e) {
//        	
//        }
//        
//        Utf8 key3 = new Utf8("key3");
//        GenericRecord value3 = new GenericRecordBuilder(schema).set("field", 1234L).build();
//        producer.send(new ProducerRecord<Object, Object>("test", key3, value3));
//        
//        producer.close();
//	}
}
