package org.z.logdumper;

import java.io.File;
import java.io.IOException;
import java.util.function.Consumer;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.DatumWriter;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class RecordWriter implements Consumer<ConsumerRecord<Object, Object>>{
	File destinationFile;
	DataFileWriter<GenericRecord> fileWriter;
	GenericRecordBuilder dumpRecordBuilder;
	
	private RecordWriter(File destinationFile) {
		this.destinationFile = destinationFile;
		// both initialized when first record arrives, because we need the schema of the values
		this.fileWriter = null; 
		this.dumpRecordBuilder = null;
	}
	
	public static RecordWriter create(String dumpDirectory, String topic, int partition) {
		File destinationFile = new File(dumpDirectory, topic + "-" + partition + ".dump");
		return new RecordWriter(destinationFile);
	}
	
	@Override
	public void accept(ConsumerRecord<Object, Object> record) {
		System.out.println("accepting record from " + record.topic() + "-" + record.partition());
		GenericRecord value = (GenericRecord) record.value();
		String key = (String) record.key();
		try {
			initFileWriterIfNeeded(value, record.topic(), record.partition());
			fileWriter.append(createDumpRecord(value, key, record.timestamp()));
		} catch (IOException e) {
			System.out.println("Failed to accept record on topic " + record.topic() + " and partition " + record.partition());
			e.printStackTrace();
		}
	}
	
	private void initFileWriterIfNeeded(GenericRecord value, String topic, int partition) throws IOException {
		if(fileWriter == null) {
			Schema schema = initSchema(value.getSchema());
			DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
			DataFileWriter<GenericRecord> fileWriter = new DataFileWriter<>(datumWriter);
			File destination = new File(System.getenv("DUMP_DIR"), topic + "-" + partition + ".dump");
			fileWriter.create(schema, destination);
			this.fileWriter = fileWriter;
			this.dumpRecordBuilder = new GenericRecordBuilder(schema);
		}
	}
	
	private Schema initSchema(Schema valueSchema) {
		return SchemaBuilder.builder().record("DumpRecord").fields()
			.name("key").type().stringType().noDefault()
			.name("value").type(valueSchema).noDefault()
			.name("timestamp").type().longType().noDefault()
			.endRecord();
	}
	
	private GenericRecord createDumpRecord(GenericRecord value, String key, long timestamp) {
		return this.dumpRecordBuilder
			.set("key", key)
			.set("value", value)
			.set("timestamp", timestamp)
			.build();
	}
}
