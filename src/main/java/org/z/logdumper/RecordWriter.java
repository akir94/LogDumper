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
import org.apache.avro.util.Utf8;
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
	
	public void write(List<ConsumerRecord<Object, Object>> records) {
		for (ConsumerRecord<Object, Object> record : records) {
			System.out.println("writing record");
			try {
				writeData(record);
			} catch (IOException e) {
				System.out.println("Failed to write ConsumerRecord to temporary buffer");
				e.printStackTrace();
			}
			System.out.println("done");
		}
		try {
			buffer.writeTo(outputStream);
		} catch (IOException e) {
			System.out.println("Failed to write temporary buffer to outputStream");
			e.printStackTrace();
		}
		buffer.reset();
	}
	
	private void writeData(ConsumerRecord<Object, Object> record) throws IOException {
		writeKeyAndValue((String) record.key(), (GenericRecord) record.value());
		bufferWriter.writeUTF(record.topic());
		bufferWriter.writeInt(record.partition());
		bufferWriter.writeLong(record.timestamp());
		bufferWriter.flush();
	}
	
	private void writeKeyAndValue(String key, GenericRecord value) throws IOException {
		datumWriter.setSchema(value.getSchema());
		datumWriter.write(value, encoder);
		if (key != null) {
			encoder.writeBoolean(true);
			encoder.writeString(key);
		} else {
			encoder.writeBoolean(false);
		}
		encoder.flush();
	}

	@Override
	public void close() throws IOException {
		outputStream.close();
		bufferWriter.close();
	}
}
