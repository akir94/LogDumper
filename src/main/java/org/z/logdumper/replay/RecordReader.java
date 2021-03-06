package org.z.logdumper.replay;

import java.io.File;
import java.io.IOException;
import java.util.function.Supplier;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.FileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;

public class RecordReader implements Supplier<GenericRecord> {
	private FileReader<GenericRecord> fileReader;
	
	private RecordReader(FileReader<GenericRecord> fileReader) {
		this.fileReader = fileReader;
	}
	
	public static RecordReader create(File file) throws IOException {
		DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
		FileReader<GenericRecord> fileReader = DataFileReader.openReader(file, datumReader);
		return new RecordReader(fileReader);
	}
	
	@Override
	public GenericRecord get() {
		return fileReader.next();
	}

}
