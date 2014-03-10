package com.linkedin.uif.source.extractor.extract;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.collect.Lists;
import com.linkedin.uif.configuration.ConfigurationKeys;
import com.linkedin.uif.configuration.SourceState;
import com.linkedin.uif.configuration.WorkUnitState;
import com.linkedin.uif.configuration.WorkUnitState.WorkingState;
import com.linkedin.uif.source.Source;
import com.linkedin.uif.source.extractor.extract.restapi.SalesforceSource;
import com.linkedin.uif.source.extractor.partition.Partitioner;
import com.linkedin.uif.source.workunit.Extract;
import com.linkedin.uif.source.workunit.WorkUnit;
import com.linkedin.uif.source.workunit.Extract.TableType;

/**
 * An implementation of Base source to get work units
 */
public abstract class BaseSource<S, D> implements Source<S, D> {
	private static final Log LOG = LogFactory.getLog(SalesforceSource.class);

    /**
     * get work units using Source state
     *
     * @param SourceState
     * @return list of work units
     */
	@Override
	public List<WorkUnit> getWorkunits(SourceState state) {
		LOG.info("Get work units");
		List<WorkUnit> workUnits = Lists.newArrayList();
		String nameSpaceName = state.getProp(ConfigurationKeys.EXTRACT_NAMESPACE_NAME_KEY);
		String entityName = state.getProp(ConfigurationKeys.SOURCE_ENTITY);
		TableType tableType = TableType.valueOf(state.getProp(ConfigurationKeys.EXTRACT_TABLE_TYPE_KEY).toUpperCase());
		long previousWatermark = this.getLatestWatermarkFromMetadata(state);
		LOG.info("Latest watermark from the previous successful runs:"+previousWatermark);
		
		Partitioner partitioner = new Partitioner(state);
		HashMap<Long, Long> partitions = partitioner.getPartitions(previousWatermark);
		Map<Long, Long> sortedPartitions = new TreeMap<Long, Long>(partitions);
		int workUnitCount = 0;
		for (Entry<Long, Long> entry : sortedPartitions.entrySet()) {
			SourceState partitionState = new SourceState();
			partitionState.addAll(state);
			partitionState.setProp(ConfigurationKeys.WORK_UNIT_LOW_WATER_MARK_KEY, entry.getKey());
			partitionState.setProp(ConfigurationKeys.WORK_UNIT_HIGH_WATER_MARK_KEY, entry.getValue());
			Extract extract = partitionState.createExtract(tableType, nameSpaceName, entityName);
			workUnits.add(partitionState.createWorkUnit(extract));
			workUnitCount++;
		}
		LOG.info("Total number of work units for the current run: "+workUnitCount);
		
		List<WorkUnit> previousWorkUnits = this.getPreviousIncompleteWorkUnits(state);
		LOG.info("Total number of work units from the previous failed runs: "+previousWorkUnits.size());
		
		workUnits.addAll(previousWorkUnits);
		return workUnits;
	}

    /**
     * get all the previous work units which are in incomplete state
     *
     * @param SourceState
     * @return list of work units
     */
	private List<WorkUnit> getPreviousIncompleteWorkUnits(SourceState state) {
		LOG.debug("Getting previous unsuccessful work units");
		List<WorkUnit> previousWorkUnits = new ArrayList<WorkUnit>();
		List<WorkUnitState> previousWorkUnitStates = state.getPreviousStates();
		if(previousWorkUnitStates.size() == 0) {
			LOG.info("No previous states for this entity");
			return previousWorkUnits;
		}
		
		for(WorkUnitState workUnitState : previousWorkUnitStates) {
			if(workUnitState.getWorkingState() == WorkingState.FAILED || workUnitState.getWorkingState() == WorkingState.ABORTED) {
				previousWorkUnits.add(workUnitState.getWorkunit());
			}
		}
		
		return previousWorkUnits;
	}

    /**
     * get latest water mark from previous states
     *
     * @param SourceState
     * @return latest water mark(low water mark)
     */
	private long getLatestWatermarkFromMetadata(SourceState state) {
		LOG.debug("Getting previous watermark");
		List<Long> highWatermarks = new ArrayList<Long>(); 
		List<WorkUnitState> previousStates = state.getPreviousStates();
		if(previousStates.size() == 0) {
			LOG.info("No previous states for this entity");
			return ConfigurationKeys.DEFAULT_WATERMARK_VALUE;
		}
		
		for(WorkUnitState workUnitState : previousStates) {
			if(workUnitState.getWorkingState() == WorkingState.COMMITTED) {
				highWatermarks.add(workUnitState.getHighWaterMark());
			}
		}
		Collections.sort(highWatermarks);
		return highWatermarks.get(highWatermarks.size() -1);
	}

	@Override
	public void shutdown(SourceState state) {
	}
}
