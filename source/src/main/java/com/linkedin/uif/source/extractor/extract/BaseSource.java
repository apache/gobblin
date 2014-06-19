package com.linkedin.uif.source.extractor.extract;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
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
import com.linkedin.uif.runtime.JobCommitPolicy;
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
		log.info("Total number of incomplete tasks from the previous run: " + previousWorkUnits.size());
		
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
		
		// Load previous failed or aborted tasks if the previous run was with commit policy COMMIT_ON_PARTIAL_SUCCESS
		// If job has commit policy as COMMIT_ON_FULL_SUCCESS and if it is failed, ignore the previous failed tasks as it is creating new tasks with the old water marks
		for(WorkUnitState workUnitState : previousWorkUnitStates) {
			JobCommitPolicy commitPolicy = JobCommitPolicy.forName(workUnitState.getWorkunit().getProp(ConfigurationKeys.JOB_COMMIT_POLICY_KEY, ConfigurationKeys.DEFAULT_JOB_COMMIT_POLICY));
			if(commitPolicy == JobCommitPolicy.COMMIT_ON_PARTIAL_SUCCESS && (workUnitState.getWorkingState() == WorkingState.FAILED || workUnitState.getWorkingState() == WorkingState.ABORTED)) {
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
		log.debug("Get latest watermark from the previou run");
		long latestWaterMark = ConfigurationKeys.DEFAULT_WATERMARK_VALUE;
		
		List<WorkUnitState> previousWorkUnitStates = state.getPreviousStates();
		List<Long> previousWorkUnitStateHighWatermarks = new ArrayList<Long>(); 
		List<Long> previousWorkUnitLowWatermarks = new ArrayList<Long>();
		
		if(previousWorkUnitStates.size() == 0) {
			log.info("Previous states are not found; Latest watermark - Default watermark: " + latestWaterMark);
			return latestWaterMark;
		}
		
		boolean hasFailedRun = false;
		boolean isCommitOnFullSuccess = false;
		
		for(WorkUnitState workUnitState : previousWorkUnitStates) {
			JobCommitPolicy commitPolicy = JobCommitPolicy.forName(workUnitState.getWorkunit().getProp(ConfigurationKeys.JOB_COMMIT_POLICY_KEY, ConfigurationKeys.DEFAULT_JOB_COMMIT_POLICY));
			log.info("Commit policy of the previous task "+workUnitState.getId()+":"+commitPolicy);
			
			if(commitPolicy == JobCommitPolicy.COMMIT_ON_FULL_SUCCESS) {
				isCommitOnFullSuccess = true;
			}
			
			log.info("State of the previous task: "+workUnitState.getId()+":"+workUnitState.getWorkingState());
			if(workUnitState.getWorkingState() == WorkingState.FAILED || workUnitState.getWorkingState() == WorkingState.ABORTED) {
				hasFailedRun = true;
			}
			
			log.info("High watermark of the previous task: "+workUnitState.getId()+":"+workUnitState.getHighWaterMark()+"\n");
			previousWorkUnitStateHighWatermarks.add(workUnitState.getHighWaterMark());
			previousWorkUnitLowWatermarks.add(this.getLowWatermarkFromWorkUnit(workUnitState));
		}
		
		// If there are no previous water marks, return default water mark 
		if(previousWorkUnitStateHighWatermarks.isEmpty()) {
			latestWaterMark = ConfigurationKeys.DEFAULT_WATERMARK_VALUE;
			log.info("Previous states are not found; Latest watermark - Default watermark: " + latestWaterMark);
		}
		
		// If commit policy is full and it has failed run, get latest water mark as minimum of low water marks from previous states
		else if(isCommitOnFullSuccess && hasFailedRun) {
			latestWaterMark = Collections.min(previousWorkUnitLowWatermarks);
			log.info("Previous job was COMMIT_ON_FULL_SUCCESS but it was failed; Latest watermark - Min watermark from WorkUnits: " + latestWaterMark);
		} 
		
		// If commit policy is full and there are no failed tasks or commit policy is partial, get latest water mark as maximum of high water marks from previous tasks
		else {
			latestWaterMark = Collections.max(previousWorkUnitStateHighWatermarks);
			log.info("Latest watermark - Max watermark from WorkUnitStates: " + latestWaterMark);
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
	private long getLowWatermarkFromWorkUnit(WorkUnitState workUnitState) {
		String watermarkType = workUnitState.getProp(ConfigurationKeys.SOURCE_QUERYBASED_WATERMARK_TYPE, ConfigurationKeys.DEFAULT_WATERMARK_TYPE);
		long lowWaterMark = workUnitState.getWorkunit().getLowWaterMark();
		
		if(lowWaterMark == ConfigurationKeys.DEFAULT_WATERMARK_VALUE) {
			return lowWaterMark;
		}
		
		WatermarkType wmType = WatermarkType.valueOf(watermarkType.toUpperCase());
		int deltaNum = new WatermarkPredicate(null, wmType).getDeltaNumForNextWatermark();
		int backupSecs = Utils.getAsInt(workUnitState.getProp(ConfigurationKeys.SOURCE_QUERYBASED_LOW_WATERMARK_BACKUP_SECS, ConfigurationKeys.DEFAULT_LOW_WATERMARK_BACKUP_SECONDS));
		
		switch(wmType) {
		case SIMPLE:
			return lowWaterMark + backupSecs - deltaNum ;
		default:
			Date lowWaterMarkDate = Utils.toDate(lowWaterMark, "yyyyMMddHHmmss");
			return Long.parseLong(Utils.dateToString(Utils.addSecondsToDate(lowWaterMarkDate, backupSecs - deltaNum), "yyyyMMddHHmmss"));
		}
	}

	@Override
	public void shutdown(SourceState state) {
	}
}
