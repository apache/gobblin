package gobblin.applift.simpleconsumer;

import gobblin.configuration.State;
import gobblin.writer.partitioner.TimeBasedWriterPartitioner;

public class TimeBasedStringWriterPartitioner extends TimeBasedWriterPartitioner<String> {

	public TimeBasedStringWriterPartitioner(State state, int numBranches, int branchId) {
		super(state, numBranches, branchId);
	}

	@Override
	public long getRecordTimestamp(String record) {
		return System.currentTimeMillis();
	}

}
