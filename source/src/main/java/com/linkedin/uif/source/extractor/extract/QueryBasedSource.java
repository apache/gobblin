package com.linkedin.uif.source.extractor.extract;

import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import com.linkedin.uif.configuration.ConfigurationKeys;
import com.linkedin.uif.configuration.SourceState;
import com.linkedin.uif.configuration.WorkUnitState;
import com.linkedin.uif.configuration.WorkUnitState.WorkingState;
import com.linkedin.uif.source.extractor.JobCommitPolicy;
import com.linkedin.uif.source.extractor.partition.Partitioner;
import com.linkedin.uif.source.extractor.utils.Utils;
import com.linkedin.uif.source.extractor.watermark.WatermarkPredicate;
import com.linkedin.uif.source.extractor.watermark.WatermarkType;
import com.linkedin.uif.source.workunit.Extract;
import com.linkedin.uif.source.workunit.WorkUnit;
import com.linkedin.uif.source.workunit.Extract.TableType;

/**
 * A base implementation of {@link com.linkedin.uif.source.Source} for query-based sources.
 */
public abstract class QueryBasedSource<S, D> extends AbstractSource<S, D> {

	private static final Logger LOG = LoggerFactory.getLogger(QueryBasedSource.class);

	@Override
	public List<WorkUnit> getWorkunits(SourceState state) {
	    initLogger(state);

		List<WorkUnit> workUnits = Lists.newArrayList();
		String nameSpaceName = state.getProp(ConfigurationKeys.EXTRACT_NAMESPACE_NAME_KEY);
		String entityName = state.getProp(ConfigurationKeys.SOURCE_ENTITY);

		String extractTableName = state.getProp(ConfigurationKeys.EXTRACT_TABLE_NAME_KEY);
		// If extract table name is not found then use the entity name
		if(StringUtils.isBlank(extractTableName)) {
			extractTableName = Utils.escapeSpecialCharacters(entityName, ConfigurationKeys.ESCAPE_CHARS_IN_TABLE_NAME, "_");
		}
		
		TableType tableType = TableType.valueOf(
                state.getProp(ConfigurationKeys.EXTRACT_TABLE_TYPE_KEY).toUpperCase());
		long previousWatermark = this.getLatestWatermarkFromMetadata(state);

		Map<Long, Long> sortedPartitions = Maps.newTreeMap();
        sortedPartitions.putAll(new Partitioner(state).getPartitions(previousWatermark));
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
		}
		LOG.info("Total number of work units for the current run: " + workUnits.size());
		
		List<WorkUnit> previousWorkUnits = this.getPreviousWorkUnitsForRetry(state);
		LOG.info("Total number of incomplete tasks from the previous run: " + previousWorkUnits.size());
		workUnits.addAll(previousWorkUnits);

		return workUnits;
	}

    @Override
    public void shutdown(SourceState state) {
    }

    /**
     * Get latest water mark from previous work unit states.
     *
     * @param state Source state
     * @return latest water mark (high water mark)
     */
	private long getLatestWatermarkFromMetadata(SourceState state) {
		LOG.debug("Get latest watermark from the previou run");
		long latestWaterMark = ConfigurationKeys.DEFAULT_WATERMARK_VALUE;
		
		List<WorkUnitState> previousWorkUnitStates = state.getPreviousStates();
		List<Long> previousWorkUnitStateHighWatermarks = Lists.newArrayList();
		List<Long> previousWorkUnitLowWatermarks = Lists.newArrayList();
		
		if(previousWorkUnitStates.isEmpty()) {
			LOG.info("No previous work unit states found; Latest watermark - Default watermark: " +
                    latestWaterMark);
			return latestWaterMark;
		}
		
		boolean hasFailedRun = false;
		boolean isCommitOnFullSuccess = false;
        JobCommitPolicy commitPolicy = JobCommitPolicy.forName(state.getProp(
                ConfigurationKeys.JOB_COMMIT_POLICY_KEY,
                ConfigurationKeys.DEFAULT_JOB_COMMIT_POLICY));
        if(commitPolicy == JobCommitPolicy.COMMIT_ON_FULL_SUCCESS) {
            isCommitOnFullSuccess = true;
        }

		for(WorkUnitState workUnitState : previousWorkUnitStates) {
			LOG.info("State of the previous task: " + workUnitState.getId() + ":" +
                    workUnitState.getWorkingState());
			if(workUnitState.getWorkingState() == WorkingState.FAILED ||
                    workUnitState.getWorkingState() == WorkingState.ABORTED) {
				hasFailedRun = true;
			}
			
			LOG.info("High watermark of the previous task: " + workUnitState.getId() + ":" +
                    workUnitState.getHighWaterMark() + "\n");
			previousWorkUnitStateHighWatermarks.add(workUnitState.getHighWaterMark());
			previousWorkUnitLowWatermarks.add(this.getLowWatermarkFromWorkUnit(workUnitState));
		}
		
		// If there are no previous water marks, return default water mark 
		if(previousWorkUnitStateHighWatermarks.isEmpty()) {
			latestWaterMark = ConfigurationKeys.DEFAULT_WATERMARK_VALUE;
			LOG.info("No previous high watermark found; Latest watermark - Default watermark: " + latestWaterMark);
		}
		
		// If commit policy is full and it has failed run, get latest water mark as
		// minimum of low water marks from previous states.
		else if(isCommitOnFullSuccess && hasFailedRun) {
			latestWaterMark = Collections.min(previousWorkUnitLowWatermarks);
			LOG.info("Previous job was COMMIT_ON_FULL_SUCCESS but it was failed; Latest watermark - " +
                    "Min watermark from WorkUnits: " + latestWaterMark);
		} 
		
		// If commit policy is full and there are no failed tasks or commit policy is partial,
		// get latest water mark as maximum of high water marks from previous tasks.
		else {
			latestWaterMark = Collections.max(previousWorkUnitStateHighWatermarks);
			LOG.info("Latest watermark - Max watermark from WorkUnitStates: " + latestWaterMark);
		}
		
		return latestWaterMark;
	}

    /**
     * Get low water mark from the given work unit state.
     *
     * @param workUnitState Work unit state
     * @return latest low water mark
     */
	private long getLowWatermarkFromWorkUnit(WorkUnitState workUnitState) {
		String watermarkType = workUnitState.getProp(
                ConfigurationKeys.SOURCE_QUERYBASED_WATERMARK_TYPE);
		long lowWaterMark = workUnitState.getWorkunit().getLowWaterMark();
		
		if(lowWaterMark == ConfigurationKeys.DEFAULT_WATERMARK_VALUE) {
			return lowWaterMark;
		}
		
		WatermarkType wmType = WatermarkType.valueOf(watermarkType.toUpperCase());
		int deltaNum = new WatermarkPredicate(null, wmType).getDeltaNumForNextWatermark();
		int backupSecs =workUnitState.getPropAsInt(
                ConfigurationKeys.SOURCE_QUERYBASED_LOW_WATERMARK_BACKUP_SECS);
		
		switch(wmType) {
		case SIMPLE:
			return lowWaterMark + backupSecs - deltaNum ;
		default:
			Date lowWaterMarkDate = Utils.toDate(lowWaterMark, "yyyyMMddHHmmss");
			return Long.parseLong(Utils.dateToString(Utils.addSecondsToDate(
                    lowWaterMarkDate, backupSecs - deltaNum), "yyyyMMddHHmmss"));
		}
	}

    /**
     * Initialize the logger.
     *
     * @param state Source state
     */
    private void initLogger(SourceState state) {
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        sb.append(StringUtils.stripToEmpty(state.getProp(ConfigurationKeys.SOURCE_QUERYBASED_SCHEMA)));
        sb.append("_");
        sb.append(StringUtils.stripToEmpty(state.getProp(ConfigurationKeys.SOURCE_ENTITY)));
        sb.append("]");
        MDC.put("sourceInfo", sb.toString());
    }
}
