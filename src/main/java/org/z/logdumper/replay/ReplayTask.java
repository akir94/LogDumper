package org.z.logdumper.replay;

import java.io.File;
import java.util.NoSuchElementException;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class ReplayTask implements Runnable {

	private KafkaProducer<Object, Object> producer;
	private Supplier<GenericRecord> supplier;
	private Function<GenericRecord, ProducerRecord<Object, Object>> converter;
	private File sourceFile;
	
	public ReplayTask(KafkaProducer<Object, Object> producer, Supplier<GenericRecord> supplier,
			Function<GenericRecord, ProducerRecord<Object, Object>> converter, File sourceFile) {
		this.producer = producer;
		this.supplier = supplier;
		this.converter = converter;
		this.sourceFile = sourceFile;
	}

	@Override
	public void run() {
		try {
			while(true) {
				GenericRecord replayRecord = supplier.get();
				ProducerRecord<Object, Object> recordToSend = converter.apply(replayRecord);
				producer.send(recordToSend);
			}
		} catch (NoSuchElementException e) {
			System.out.println("Finished processing messages from file " + sourceFile);
		} catch (RuntimeException e) {
			System.out.println("Failed to process file " + sourceFile);
			e.printStackTrace();
		}
	}

}
