package com.linkedin.uif.source.extractor.datapurger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
import com.linkedin.uif.source.extractor.filebased.FileBasedHelper;
import com.linkedin.uif.source.extractor.filebased.FileBasedHelperException;
import com.linkedin.uif.source.extractor.filebased.FileBasedSource;
import com.linkedin.uif.source.extractor.hadoop.HadoopFsHelper;
import com.linkedin.uif.source.workunit.Extract;
import com.linkedin.uif.source.workunit.WorkUnit;
import com.linkedin.uif.source.workunit.Extract.TableType;

public class DataPurgerSource extends FileBasedSource<Schema, GenericRecord>
{
    private static final Logger log = LoggerFactory.getLogger(FileBasedSource.class);
    
    private static final String DATA_PURGER_WHITELIST = "data.purger.whitelist";
    private static final String DATA_PURGER_INPUT_PATH = "data.purger.input.path";
    private static final String DAILY_FOLDER = "daily";
        
    @Override
    public Extractor<Schema, GenericRecord> getExtractor(WorkUnitState state) throws IOException
    {
        return new DataPurgerExtractor(state);
    }

    @Override
    public FileBasedHelper initFileSystemHelper(State state)
    {
        FileBasedHelper fsHelper = new HadoopFsHelper(state);
        try
        {
            fsHelper.connect();
        }
        catch (FileBasedHelperException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return fsHelper;
    }
    
    @Override
    public List<WorkUnit> getWorkunits(SourceState state)
    {
        initLogger(state);
        this.fsHelper = initFileSystemHelper(state);
        
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

        List<String> filesToPull = new ArrayList<String>();
        List<String> whitelist = state.getPropAsList(DATA_PURGER_WHITELIST);
        HadoopFsHelper fsHelper = (HadoopFsHelper) this.fsHelper;
        FileSystem fs = fsHelper.getFileSystem();
        
        try {
            for (FileStatus status : fs.listStatus(new Path(state.getProp(DATA_PURGER_INPUT_PATH)))) {
                String topicName = status.getPath().getName();
                if (whitelist.contains(topicName)) {
                    addAllInputFiles(fs, new Path(status.getPath(), DAILY_FOLDER), filesToPull);
                }
            }
        } catch (IOException e) {
            Throwables.propagate(e);
        }
        
        int workUnitCount = 0;
        List<WorkUnit> workUnits = Lists.newArrayList();
        for (String file : filesToPull) {
            SourceState partitionState = new SourceState();
            partitionState.addAll(state);
            partitionState.setProp(ConfigurationKeys.SOURCE_FILEBASED_FILES_TO_PULL, file);

            if (state.getPropAsBoolean(ConfigurationKeys.SOURCE_FILEBASED_PRESERVE_FILE_PATH, false)) {
                partitionState.setProp(ConfigurationKeys.DATA_PUBLISHER_FINAL_DIR, new Path(file).getParent().toString());
                partitionState.setProp(ConfigurationKeys.DATA_PUBLISHER_FINAL_NAME, new Path(file).getName().toString());
            }

            // Use extract table name to create extract
            Extract extract = partitionState.createExtract(tableType, nameSpaceName, extractTableName);
            workUnits.add(partitionState.createWorkUnit(extract));
            workUnitCount++;
        }
        
        log.info("Total number of work units for the current run: " + workUnitCount);        
        return workUnits;
    }
    
    public void addAllInputFiles(FileSystem fs, Path path, List<String> filesToPull) throws IOException {
        for (FileStatus status : fs.listStatus(path)) {
            if (status.isDir()) {
                addAllInputFiles(fs, status.getPath(), filesToPull);
            } else {
                filesToPull.add(status.getPath().toString());
            }
        }
    }
}
