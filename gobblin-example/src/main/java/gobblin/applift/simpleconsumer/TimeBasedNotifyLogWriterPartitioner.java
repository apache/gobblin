package gobblin.applift.simpleconsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gobblin.configuration.State;
import gobblin.writer.partitioner.TimeBasedWriterPartitioner;

public class TimeBasedNotifyLogWriterPartitioner extends TimeBasedWriterPartitioner<String> {

	private static final Logger LOG = LoggerFactory.getLogger(TimeBasedNotifyLogWriterPartitioner.class);
	
	public TimeBasedNotifyLogWriterPartitioner(State state, int numBranches, int branchId) {
		super(state, numBranches, branchId);
	}

	@Override
	public long getRecordTimestamp(String record) {
		return System.currentTimeMillis();
	}

}
