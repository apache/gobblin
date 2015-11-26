package gobblin.applift.parquet;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericRecord;

import gobblin.configuration.State;
import gobblin.writer.FsDataWriter;
import gobblin.writer.FsDataWriterBuilder;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;

public class ParquetHdfsDataWriter extends FsDataWriter<GenericRecord> {
	private final AtomicLong count = new AtomicLong(0);
	private ParquetWriter<GenericRecord> writer;
	public ParquetHdfsDataWriter(FsDataWriterBuilder<Schema, GenericRecord> builder, State properties, Schema schema)
	    throws IOException {
		super(builder, properties);
		this.writer = this.closer.register(createDataFileWriter(schema));
	}

	@Override
	public void write(GenericRecord record) throws IOException {
		// TODO Auto-generated method stub
		this.writer.write(record);
		this.count.incrementAndGet();
	}

	@Override
	public long recordsWritten() {
		return this.count.get();
	}

	@Override
	public long bytesWritten() throws IOException {
		if (!this.fs.exists(this.outputFile)) {
			return 0;
		}

		return this.fs.getFileStatus(this.outputFile).getLen();
	}

	/**
   * Create a new {@link ParquetWriter} for writing Avro records.
   *
   * @param codecFactory a {@link CodecFactory} object for building the compression codec
   * @throws IOException if there is something wrong creating a new {@link DataFileWriter}
   */
  private org.apache.parquet.hadoop.ParquetWriter<GenericRecord> createDataFileWriter(Schema schema) throws IOException {
    return AvroParquetWriter.<GenericRecord>builder(this.stagingFile).withSchema(schema).build();
  }

}
