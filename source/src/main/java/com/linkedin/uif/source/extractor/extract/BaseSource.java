package com.linkedin.uif.source.extractor.extract;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;

import com.linkedin.uif.configuration.ConfigurationKeys;
import com.linkedin.uif.configuration.SourceState;
import com.linkedin.uif.configuration.WorkUnitState;
import com.linkedin.uif.configuration.WorkUnitState.WorkingState;
import com.linkedin.uif.source.Source;
import com.linkedin.uif.source.extractor.partition.Partitioner;
import com.linkedin.uif.source.extractor.utils.Utils;
import com.linkedin.uif.source.extractor.watermark.WatermarkPredicate;
import com.linkedin.uif.source.extractor.watermark.WatermarkType;
import com.linkedin.uif.source.workunit.Extract;
import com.linkedin.uif.source.workunit.WorkUnit;
import com.linkedin.uif.source.workunit.Extract.TableType;

/**
 * An implementation of Base source to get work units
 */
public abstract class BaseSource<S, D> implements Source<S, D> {
	private static final Logger log = LoggerFactory.getLogger(BaseSource.class);
	
	public void initLogger(SourceState state) {
	    StringBuilder sb = new StringBuilder();
	    sb.append("[");
	    sb.append(Strings.nullToEmpty(state.getProp(ConfigurationKeys.SOURCE_QUERYBASED_SCHEMA)));
	    sb.append("_");
	    sb.append(Strings.nullToEmpty(state.getProp(ConfigurationKeys.SOURCE_ENTITY)));
	    sb.append("]");
	    MDC.put("tableName", sb.toString());
	}

    /**
     * get work units using Source state
     *
     * @param SourceState
     * @return list of work units
     */
	@Override
	public List<WorkUnit> getWorkunits(SourceState state) {
	    initLogger(state);
		log.info("Get work units");
		List<WorkUnit> workUnits = Lists.newArrayList();
		String nameSpaceName = state.getProp(ConfigurationKeys.EXTRACT_NAMESPACE_NAME_KEY);
		String entityName = state.getProp(ConfigurationKeys.SOURCE_ENTITY);
		
		// Override extract table name
		String extractTableName = state.getProp(ConfigurationKeys.EXTRACT_TABLE_NAME_KEY);
		
		// if extract table name is not found then consider entity name as extract table name
		if(Strings.isNullOrEmpty(extractTableName)) {
			extractTableName = entityName;
		}
		
		TableType tableType = TableType.valueOf(state.getProp(ConfigurationKeys.EXTRACT_TABLE_TYPE_KEY).toUpperCase());
		long previousWatermark = this.getLatestWatermarkFromMetadata(state);

		Partitioner partitioner = new Partitioner(state);
		HashMap<Long, Long> partitions = partitioner.getPartitions(previousWatermark);
		Map<Long, Long> sortedPartitions = new TreeMap<Long, Long>(partitions);
		int workUnitCount = 0;
		for (Entry<Long, Long> entry : sortedPartitions.entrySet()) {
			SourceState partitionState = new SourceState();
			partitionState.addAll(state);
			partitionState.setProp(ConfigurationKeys.WORK_UNIT_LOW_WATER_MARK_KEY, entry.getKey());
			partitionState.setProp(ConfigurationKeys.WORK_UNIT_HIGH_WATER_MARK_KEY, entry.getValue());
			
			// Use extract table name to create extract
			Extract extract = partitionState.createExtract(tableType, nameSpaceName, extractTableName);
			
			// setting current time for the full extract
			if(Boolean.valueOf(state.getProp(ConfigurationKeys.EXTRACT_IS_FULL_KEY))) {
				extract.setFullTrue(System.currentTimeMillis());
			}
			workUnits.add(partitionState.createWorkUnit(extract));
			workUnitCount++;
		}
		log.info("Total number of work units for the current run: " + workUnitCount);
		
		List<WorkUnit> previousWorkUnits = this.getPreviousIncompleteWorkUnits(state);
		log.info("Total number of work units from the previous failed runs: " + previousWorkUnits.size());
		
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
		log.debug("Getting previous unsuccessful work units");
		List<WorkUnit> previousWorkUnits = new ArrayList<WorkUnit>();
		List<WorkUnitState> previousWorkUnitStates = state.getPreviousStates();
		if(previousWorkUnitStates.size() == 0) {
			log.debug("Previous states are not found");
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
     * get latest water mark from previous work unit states
     *
     * @param SourceState
     * @return latest water mark(high water mark of WorkUnitState)
     */
	private long getLatestWatermarkFromMetadata(SourceState state) {
		log.debug("Getting previous watermark");
		boolean hasDataInPreviousRun = false;
		long latestWaterMark = ConfigurationKeys.DEFAULT_WATERMARK_VALUE;
		
		List<WorkUnitState> previousWorkUnitStates = state.getPreviousStates();
		List<Long> previousWorkUnitStateHighWatermarks = new ArrayList<Long>(); 
		List<Long> previousWorkUnitLowWatermarks = new ArrayList<Long>();
		
		if(previousWorkUnitStates.size() == 0) {
			log.info("Previous states are not found; Latest watermark - Default watermark: " + latestWaterMark);
			return latestWaterMark;
		}
		
		for(WorkUnitState workUnitState : previousWorkUnitStates) {
			if(workUnitState.getWorkingState() == WorkingState.COMMITTED) {
				if(workUnitState.getHighWaterMark() != ConfigurationKeys.DEFAULT_WATERMARK_VALUE) {
					hasDataInPreviousRun = true;
				}
				previousWorkUnitStateHighWatermarks.add(workUnitState.getHighWaterMark());
				previousWorkUnitLowWatermarks.add(this.getLowWatermarkFromWorkUnit(workUnitState.getWorkunit(), workUnitState.getProp(ConfigurationKeys.SOURCE_QUERYBASED_WATERMARK_TYPE)));
			}
		}
		
		if(previousWorkUnitStateHighWatermarks.isEmpty()) {
			latestWaterMark = ConfigurationKeys.DEFAULT_WATERMARK_VALUE;
			log.info("Previous Committed states are not found; Latest watermark - Default watermark: " + latestWaterMark);
		} else if(hasDataInPreviousRun) {
			latestWaterMark = Collections.max(previousWorkUnitStateHighWatermarks);
			log.info("Previous run has data; Latest watermark - Max watermark from WorkUnitStates: " + latestWaterMark);
		} else {
			latestWaterMark = Collections.min(previousWorkUnitLowWatermarks);
			log.info("Previous run has no data; Latest watermark - Min watermark from WorkUnits: " + latestWaterMark);
		}
		
		return latestWaterMark;
	}

    /**
     * get latest water mark from previous work units
     *
     * @param work unit
     * @param watermark type
     * @return latest water mark(low water mark of WorkUnit)
     */
	private long getLowWatermarkFromWorkUnit(WorkUnit workunit, String watermarkType) {
		if(workunit.getLowWaterMark() == ConfigurationKeys.DEFAULT_WATERMARK_VALUE) {
			return ConfigurationKeys.DEFAULT_WATERMARK_VALUE;
		}
		
		WatermarkType wmType = WatermarkType.valueOf(watermarkType.toUpperCase());
		int deltaNum = new WatermarkPredicate(null, wmType).getDeltaNumForNextWatermark();
		
		if(wmType == WatermarkType.SIMPLE) {
			return workunit.getLowWaterMark() - deltaNum;
		} else {
			return Long.parseLong(Utils.dateToString(Utils.addSecondsToDate(Utils.toDate(workunit.getLowWaterMark(), "yyyyMMddHHmmss"),deltaNum*-1), "yyyyMMddHHmmss"));
		}
	}

	@Override
	public void shutdown(SourceState state) {
	}
}
