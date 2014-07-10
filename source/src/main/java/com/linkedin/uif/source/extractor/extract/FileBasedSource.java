package com.linkedin.uif.source.extractor.extract;

import java.util.Arrays;
import java.util.List;

import com.linkedin.uif.source.extractor.extract.AbstractSource;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;

import com.linkedin.uif.configuration.ConfigurationKeys;
import com.linkedin.uif.configuration.SourceState;
import com.linkedin.uif.configuration.State;
import com.linkedin.uif.configuration.WorkUnitState;
import com.linkedin.uif.source.workunit.Extract;
import com.linkedin.uif.source.workunit.WorkUnit;
import com.linkedin.uif.source.workunit.Extract.TableType;

/**
 * This class is a base class for file based sources, it provides default
 * functionality for keeping track of which files have already been pulled
 * by the framework and for determing which files need to be pulled in this run
 * @author stakiar
 */
public abstract class FileBasedSource<K, V> extends AbstractSource<K, V>
{  
    private static final Logger log = LoggerFactory.getLogger(FileBasedSource.class);
    
    /**
     * Initialize the logger.
     *
     * @param state Source state
     */
    private void initLogger(SourceState state) {
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        sb.append(Strings.nullToEmpty(state.getProp(ConfigurationKeys.SOURCE_ENTITY)));
        sb.append("]");
        MDC.put("sourceInfo", sb.toString());
    }
    
    /**
     * This method takes the snapshot seen in the previous run, and compares it to the list
     * of files currently in the source - it then decided which files it needs to pull
     * and distributes those files across the workunits; it does this comparison by comparing
     * the names of the files currently in the source vs. the names retrieved from the 
     * previous state
     * @param state is the source state
     * @return a list of workunits for the framework to run
     */
    @Override
    public List<WorkUnit> getWorkunits(SourceState state)
    {
        initLogger(state);
        
        log.info("Getting work units");
        String nameSpaceName = state.getProp(ConfigurationKeys.EXTRACT_NAMESPACE_NAME_KEY);
        String entityName = state.getProp(ConfigurationKeys.SOURCE_ENTITY);

        // Override extract table name
        String extractTableName = state.getProp(ConfigurationKeys.EXTRACT_TABLE_NAME_KEY);

        // If extract table name is not found then consider entity name as extract table name
        if (Strings.isNullOrEmpty(extractTableName)) {
            extractTableName = entityName;
        }

        TableType tableType = TableType.valueOf(state.getProp(ConfigurationKeys.EXTRACT_TABLE_TYPE_KEY).toUpperCase());        
        List<WorkUnitState> previousWorkunits = state.getPreviousStates();
        List<String> prevFsSnapshot = Lists.newArrayList();

        // Get list of files seen in the previous run
        if (!previousWorkunits.isEmpty() && previousWorkunits.get(0).getWorkunit().contains(ConfigurationKeys.SOURCE_FILEBASED_FS_SNAPSHOT)) {
            prevFsSnapshot = previousWorkunits.get(0).getWorkunit().getPropAsList(ConfigurationKeys.SOURCE_FILEBASED_FS_SNAPSHOT);
        }

        // Get list of files that need to be pulled
        List<String> currentFsSnapshot = this.getcurrentFsSnapshot(state);
        List<String> filesToPull = Lists.newArrayList(currentFsSnapshot);
        filesToPull.removeAll(prevFsSnapshot);

        log.info("Will pull the following files in this run: " + Arrays.toString(filesToPull.toArray()));

        int numPartitions = state.contains((ConfigurationKeys.SOURCE_MAX_NUMBER_OF_PARTITIONS)) &&
                            state.getPropAsInt(ConfigurationKeys.SOURCE_MAX_NUMBER_OF_PARTITIONS) <= filesToPull.size() ?
                            state.getPropAsInt(ConfigurationKeys.SOURCE_MAX_NUMBER_OF_PARTITIONS) : filesToPull.size();
        int filesPerPartition = (numPartitions == 0) ? 0 : (int) Math.ceil(filesToPull.size() / numPartitions);
        int workUnitCount = 0;
        int fileOffset = 0;
        
        // Distribute the files across the workunits
        List<WorkUnit> workUnits = Lists.newArrayList();
        for (int i = 0; i < numPartitions; i++) {
            SourceState partitionState = new SourceState();
            partitionState.addAll(state);
            
            // Eventually these setters should be integrated with framework support for generalized watermark handling
            partitionState.setProp(ConfigurationKeys.SOURCE_FILEBASED_FS_SNAPSHOT, StringUtils.join(currentFsSnapshot, ","));
            partitionState.setProp(ConfigurationKeys.SOURCE_FILEBASED_FILES_TO_PULL, StringUtils.join(filesToPull.subList(fileOffset, fileOffset + filesPerPartition > filesToPull.size() ? filesToPull.size() : fileOffset + filesPerPartition), ","));
            
            // Use extract table name to create extract
            Extract extract = partitionState.createExtract(tableType, nameSpaceName, extractTableName);
            workUnits.add(partitionState.createWorkUnit(extract));
            workUnitCount++;
            fileOffset += filesPerPartition;
        }
        
        log.info("Total number of work units for the current run: " + workUnitCount);
        
        List<WorkUnit> previousWorkUnits = this.getPreviousWorkUnitsForRetry(state);
        log.info("Total number of work units from the previous failed runs: " + previousWorkUnits.size());
        
        workUnits.addAll(previousWorkUnits);
        return workUnits;
    }
    
    /**
     * This method is responsible for connecting to the source and taking
     * a snapshot of the folder where the data is present, it then returns
     * a list of the files in String format
     * @param state is used to connect to the source
     * @return a list of file name or paths present on the external data
     * directory
     */
    public abstract List<String> getcurrentFsSnapshot(State state);
}
