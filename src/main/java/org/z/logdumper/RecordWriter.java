package org.z.logdumper;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class RecordWriter implements Closeable{
	private OutputStream outputStream;
	private ByteArrayOutputStream buffer;
	private DataOutputStream bufferWriter;
	private BinaryEncoder encoder;
	private DatumWriter<GenericRecord> datumWriter;
	
	public RecordWriter(OutputStream outputStream) {
		this.outputStream = outputStream;
		this.buffer = new ByteArrayOutputStream();
		this.bufferWriter = new DataOutputStream(buffer);
		this.encoder = EncoderFactory.get().binaryEncoder(bufferWriter, null);
		this.datumWriter = new GenericDatumWriter<>();
	}
	
	public void write(List<ConsumerRecord<GenericRecord, GenericRecord>> records) {
		for (ConsumerRecord<GenericRecord, GenericRecord> record : records) {
			try {
				datumWriter.write(record.value(), encoder);
				datumWriter.write(record.key(), encoder);
				encoder.flush();
				bufferWriter.writeUTF(record.topic());
				bufferWriter.writeInt(record.partition());
				bufferWriter.writeLong(record.timestamp());
				bufferWriter.flush();
			} catch (IOException e) {
				System.out.println("Failed to write ConsumerRecord to temporary buffer");
				e.printStackTrace();
			}
		}
		try {
			buffer.writeTo(outputStream);
		} catch (IOException e) {
			System.out.println("Failed to write temporary buffer to outputStream");
			e.printStackTrace();
		}
		buffer.reset();
	}

	@Override
	public void close() throws IOException {
		outputStream.close();
		bufferWriter.close();
	}
}
