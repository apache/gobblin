package gobblin.applift.parquet;

import org.apache.avro.generic.GenericRecord;

import gobblin.configuration.State;
import gobblin.writer.partitioner.TimeBasedWriterPartitioner;

public class ParquetTimeBasedWriterPartitioner extends TimeBasedWriterPartitioner<GenericRecord> {

	public ParquetTimeBasedWriterPartitioner(State state, int numBranches, int branchId) {
		super(state, numBranches, branchId);
		
	}

	@Override
	public long getRecordTimestamp(GenericRecord record) {
		return System.currentTimeMillis();
	}

}
