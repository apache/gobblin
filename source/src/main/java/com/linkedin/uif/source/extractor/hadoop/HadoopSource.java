package com.linkedin.uif.source.extractor.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.linkedin.uif.configuration.ConfigurationKeys;
import com.linkedin.uif.configuration.SourceState;
import com.linkedin.uif.configuration.State;
import com.linkedin.uif.configuration.WorkUnitState;
import com.linkedin.uif.source.extractor.Extractor;
import com.linkedin.uif.source.extractor.filebased.FileBasedHelperException;
import com.linkedin.uif.source.extractor.filebased.FileBasedSource;
import com.linkedin.uif.source.workunit.Extract;
import com.linkedin.uif.source.workunit.Extract.TableType;
import com.linkedin.uif.source.workunit.WorkUnit;

public class HadoopSource extends FileBasedSource<Schema, GenericRecord> {
    private Logger log = LoggerFactory.getLogger(HadoopSource.class);

    @Override
    public Extractor<Schema, GenericRecord> getExtractor(WorkUnitState state) throws IOException {
        return new HadoopExtractor(state);
    }

    @Override
    public void initFileSystemHelper(State state) throws FileBasedHelperException {
        this.fsHelper = new HadoopFsHelper(state);
        this.fsHelper.connect();
    }

    @Override
    public List<String> getcurrentFsSnapshot(State state) {
        List<String> results = new ArrayList<String>();
        String path = state.getProp(ConfigurationKeys.SOURCE_FILEBASED_DATA_DIRECTORY);

        try {
            log.info("Running ls command with input " + path);
            results = this.fsHelper.ls(path);
        } catch (FileBasedHelperException e) {
            log.error("Not able to run ls command due to " + e.getMessage()
                    + " will not pull any files", e);
        }
        return results;
    }

    @Override
    public List<WorkUnit> getWorkunits(SourceState state) {
        initLogger(state);
        try {
            initFileSystemHelper(state);
        } catch (FileBasedHelperException e) {
            Throwables.propagate(e);
        }

        log.info("Getting work units");
        String nameSpaceName = state.getProp(ConfigurationKeys.EXTRACT_NAMESPACE_NAME_KEY);
        String entityName = state.getProp(ConfigurationKeys.SOURCE_ENTITY);

        // Override extract table name
        String extractTableName = state.getProp(ConfigurationKeys.EXTRACT_TABLE_NAME_KEY);

        // If extract table name is not found then consider entity name as
        // extract table name
        if (Strings.isNullOrEmpty(extractTableName)) {
            extractTableName = entityName;
        }

        TableType tableType = TableType.valueOf(state.getProp(
                ConfigurationKeys.EXTRACT_TABLE_TYPE_KEY).toUpperCase());
        List<WorkUnitState> previousWorkunits = state.getPreviousStates();
        List<String> prevFsSnapshot = Lists.newArrayList();

        // Get list of files seen in the previous run
        if (!previousWorkunits.isEmpty()
                && previousWorkunits.get(0).getWorkunit()
                        .contains(ConfigurationKeys.SOURCE_FILEBASED_FS_SNAPSHOT)) {
            prevFsSnapshot = previousWorkunits.get(0).getWorkunit()
                    .getPropAsList(ConfigurationKeys.SOURCE_FILEBASED_FS_SNAPSHOT);
        }

        // Get list of files that need to be pulled
        List<String> currentFsSnapshot = this.getcurrentFsSnapshot(state);
        List<String> filesToPull = Lists.newArrayList(currentFsSnapshot);
        filesToPull.removeAll(prevFsSnapshot);

        log.info("Will pull the following files in this run: "
                + Arrays.toString(filesToPull.toArray()));

        int numPartitions = state.contains((ConfigurationKeys.SOURCE_MAX_NUMBER_OF_PARTITIONS))
                && state.getPropAsInt(ConfigurationKeys.SOURCE_MAX_NUMBER_OF_PARTITIONS) <= filesToPull
                        .size() ? state
                .getPropAsInt(ConfigurationKeys.SOURCE_MAX_NUMBER_OF_PARTITIONS) : filesToPull
                .size();
        int filesPerPartition = (numPartitions == 0) ? 0 : (int) Math.ceil(filesToPull.size()
                / numPartitions);
        int workUnitCount = 0;
        int fileOffset = 0;

        // Distribute the files across the workunits
        List<WorkUnit> workUnits = Lists.newArrayList();
        for (int i = 0; i < numPartitions; i++) {
            SourceState partitionState = new SourceState();
            partitionState.addAll(state);

            // Eventually these setters should be integrated with framework
            // support for generalized watermark handling
            partitionState.setProp(ConfigurationKeys.SOURCE_FILEBASED_FS_SNAPSHOT,
                    StringUtils.join(currentFsSnapshot, ","));

            List<String> partitionFilesToPull = filesToPull.subList(fileOffset, fileOffset
                    + filesPerPartition > filesToPull.size() ? filesToPull.size() : fileOffset
                    + filesPerPartition);
            partitionState.setProp(ConfigurationKeys.SOURCE_FILEBASED_FILES_TO_PULL,
                    StringUtils.join(partitionFilesToPull, ","));
            if (state
                    .getPropAsBoolean(ConfigurationKeys.SOURCE_FILEBASED_PRESERVE_FILE_PATH, false)) {
                if (partitionFilesToPull.size() != 1) {
                    throw new RuntimeException(
                            "Cannot preserve the file name if a workunit is given multiple files");
                }
                partitionState.setProp(ConfigurationKeys.DATA_PUBLISHER_FINAL_DIR,
                        partitionState.getProp(ConfigurationKeys.SOURCE_FILEBASED_FILES_TO_PULL));
            }

            // Use extract table name to create extract
            Extract extract = partitionState.createExtract(tableType, nameSpaceName,
                    extractTableName);
            workUnits.add(partitionState.createWorkUnit(extract));
            workUnitCount++;
            fileOffset += filesPerPartition;
        }

        log.info("Total number of work units for the current run: " + workUnitCount);

        List<WorkUnit> previousWorkUnits = this.getPreviousWorkUnitsForRetry(state);
        log.info("Total number of work units from the previous failed runs: "
                + previousWorkUnits.size());

        workUnits.addAll(previousWorkUnits);
        return workUnits;
    }
}
