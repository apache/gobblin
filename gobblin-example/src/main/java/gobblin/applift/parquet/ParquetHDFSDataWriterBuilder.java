package gobblin.applift.parquet;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import gobblin.configuration.State;
import gobblin.writer.DataWriter;
import gobblin.writer.FsDataWriterBuilder;
import gobblin.writer.WriterOutputFormat;

/*
 * 
 * @author prashant.bhardwaj@applift.com
 * 
 */

public class ParquetHDFSDataWriterBuilder extends FsDataWriterBuilder<Schema, GenericRecord> {

	@Override
	public DataWriter<GenericRecord> build() throws IOException {
		Preconditions.checkNotNull(this.destination);
		Preconditions.checkArgument(!Strings.isNullOrEmpty(this.writerId));
		Preconditions.checkNotNull(this.schema);
		Preconditions.checkArgument(this.format == WriterOutputFormat.PARQUET);
		switch (this.destination.getType()) {
		case HDFS:
			State properties = this.destination.getProperties();
			return new ParquetHdfsDataWriter(this, properties, this.schema);

		default:
			throw new RuntimeException("Unknown destination type: " + this.destination.getType());
		}
	}

}
