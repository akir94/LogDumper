package org.z.logdumper.replay;

import java.io.File;
import java.time.Duration;
import java.time.Instant;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * Replays messages by reading them from a supplier, converting them to a ProducerRecord,
 * sending them to a KafkaProducer.
 * 
 * Also sleeps between sending the messages, to better simulate the original execution.
 * 
 * @author akir94
 *
 */
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
		SleepTimer sleepTimer = new SleepTimer();
		try {
			while (true) {
				GenericRecord replayRecord = supplier.get();
				ProducerRecord<Object, Object> recordToSend = converter.apply(replayRecord);
				Instant currentRecordTimestamp = Instant.ofEpochMilli((long) replayRecord.get("timestamp"));
				sleepTimer.sleepUntilNextSend(currentRecordTimestamp);
				producer.send(recordToSend);
				sleepTimer.updateTimestamps(currentRecordTimestamp, Instant.now());
			}
		} catch (NoSuchElementException e) {
			System.out.println("Finished processing messages from file " + sourceFile);
		} catch (RuntimeException e) {
			System.out.println("Failed to process file " + sourceFile);
			e.printStackTrace();
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
	}
	
	private static class SleepTimer {
		private Instant lastRecordTimestamp;  // the timestamp of the last message we sent
		private Instant lastSendInstant;  // the instant we sent the previous message
		
		public void sleepUntilNextSend(Instant currentRecordTimestamp) throws InterruptedException {
			if (lastSendInstant == null || lastRecordTimestamp == null) { // first send
				return;
			} else {
				Objects.requireNonNull(lastRecordTimestamp);
				Objects.requireNonNull(lastSendInstant);

				Duration requiredWaitTime = Duration.between(lastRecordTimestamp, currentRecordTimestamp);
				Duration elapsedTime = Duration.between(lastSendInstant, Instant.now());
				Duration sleepDuration = requiredWaitTime.minus(elapsedTime);
				if (sleepDuration.isNegative()) {
					return;
				} else {
					Thread.sleep(sleepDuration.toMillis());
				}
			}
		}
		
		public void updateTimestamps(Instant recordTimestamp, Instant sendInstant) {
			this.lastRecordTimestamp = recordTimestamp;
			this.lastSendInstant = sendInstant;
		}
	}

}
