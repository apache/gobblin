package gobblin.applift.parquet;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericRecord;

import gobblin.configuration.State;
import gobblin.writer.FsDataWriter;
import gobblin.writer.FsDataWriterBuilder;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;

/*
 * @author prashant.bhardwaj@applift.com
 * 
 */

public class ParquetHdfsDataWriter extends FsDataWriter<GenericRecord> {
	private final AtomicLong count = new AtomicLong(0);
	private ParquetWriter<GenericRecord> writer;
	public ParquetHdfsDataWriter(FsDataWriterBuilder<Schema, GenericRecord> builder, State properties, Schema schema)
	    throws IOException {
		super(builder, properties);
		this.writer = this.closer.register(createParquetWriter(schema));
	}

	@Override
	public void write(GenericRecord record) throws IOException {
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
   * Create a new {@link org.apache.parquet.hadoop.ParquetWriter} for writing Avro records.
   *
   * @param schema {@link org.apache.avro.Schema} for writing avro record to Parquet file.
   * @throws IOException if there is something wrong creating a new {@link DataFileWriter}
   */
  private org.apache.parquet.hadoop.ParquetWriter<GenericRecord> createParquetWriter(Schema schema) throws IOException {
    return AvroParquetWriter.<GenericRecord>builder(this.stagingFile).withSchema(schema).build();
  }

}
