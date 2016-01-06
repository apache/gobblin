package gobblin.applift.simpleconsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import gobblin.configuration.State;
import gobblin.writer.partitioner.TimeBasedWriterPartitioner;

public class TimeBasedReqLogWriterPartitioner extends TimeBasedWriterPartitioner<String> {
	private static final Logger LOG = LoggerFactory.getLogger(TimeBasedReqLogWriterPartitioner.class);
	
	public TimeBasedReqLogWriterPartitioner(State state, int numBranches, int branchId) {
		super(state, numBranches, branchId);
	}

	@Override
	public long getRecordTimestamp(String record) {
		JsonElement element = new JsonParser().parse(record);
		JsonObject reqLogObject = element.getAsJsonObject();
		JsonObject reqInfoObject = reqLogObject.getAsJsonObject("req_info");
		if(reqInfoObject.get("unix_ts").toString() != null)
			LOG.warn("Applift: FaultyRecord = "+record);
		float unixTS = Float.valueOf(reqInfoObject.get("unix_ts").toString());
		long timestampMS = (long) (unixTS*1000);
		return timestampMS;
	}
}
