package com.linkedin.uif.source.extractor.extract.sftp;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.uif.configuration.ConfigurationKeys;
import com.linkedin.uif.configuration.WorkUnitState;
import com.linkedin.uif.source.extractor.DataRecordException;
import com.linkedin.uif.source.extractor.exception.HighWatermarkException;
import com.linkedin.uif.source.extractor.exception.RecordCountException;
import com.linkedin.uif.source.extractor.exception.SchemaException;
import com.linkedin.uif.source.extractor.extract.Command;
import com.linkedin.uif.source.extractor.extract.CommandOutput;
import com.linkedin.uif.source.extractor.extract.sftp.SftpCommand.SftpCommandType;
import com.linkedin.uif.source.extractor.watermark.Predicate;
import com.linkedin.uif.source.extractor.watermark.WatermarkType;
import com.linkedin.uif.source.workunit.WorkUnit;

/**
 * Extractor to pull Responsys data using the SFTP protocol
 * @author stakiar
 *
 * @param <D> type of data record
 * @param <S> type of schema
 */
public class ResponsysExtractor extends SftpExtractor
{
    private static final Logger log = LoggerFactory.getLogger(ResponsysExtractor.class);
    
    private static final String RESPONSYS_SCHEMA = "responsys.schema";
    private static final String RESPONSYS_DECRYPT_KEY = "responsys.decrypt.key.location";

    
    private Iterator<String> filesToPull;
    private String currentFile;
    private InputStream decryptedInputStream;
    private InputStream sftpInputStream;

    /**
     * filesToPull is a list of files to pull from Responsys
     * This list is created by ResponsysSource and is passed
     * through the workUnitState
     * @param workUnitState is the state object for this extractor
     */
    public ResponsysExtractor(WorkUnitState workUnitState)
    {
        super(workUnitState);
        this.filesToPull = new ArrayList<String>(workUnitState.getPropAsList(ResponsysSource.RESPONSYS_FILES_TO_PULL)).iterator();
    }

    /**
     * The schema comes from the config, so there are no SFTP commands to executes
     * to get the schema
     */
    @Override
    public List<Command> getSchemaMetadata(String schema, String entity) throws SchemaException
    {
        return new ArrayList<Command>();
    }

    /**
     * No SFTP commands to get the schema, so no output is produced. The schema comes
     * from the config and is returned as a String
     */
    @Override
    public String getSchema(CommandOutput<?, ?> response) throws SchemaException, IOException
    {
        return this.workUnit.getProp(RESPONSYS_SCHEMA);
    }

    /**
     * We do not need to get the high watermark for each extractor, as it is already
     * determined in the Source class, so return a list of 0 commands
     */
    @Override
    public List<Command> getHighWatermarkMetadata(String schema, String entity, String watermarkColumn, List<Predicate> predicateList) throws HighWatermarkException
    {
        return new ArrayList<Command>();
    }

    /**
     * No commands executed, so response is empty. Watermark object is not used for
     * Responsys so return -1
     */
    @Override
    public long getHighWatermark(CommandOutput<?, ?> response, String watermarkColumn, String predicateColumnFormat) throws HighWatermarkException
    {
       return -1;
    }

    /**
     * No-op because we cannot get the count until we have decrypted the file,
     * but the file decryption operation returns an InputStream to the file
     * so we cannot get the count
     */
    @Override
    public List<Command> getCountMetadata(String schema, String entity, WorkUnit workUnit, List<Predicate> predicateList) throws RecordCountException
    {
        return new ArrayList<Command>();
    }

    /**
     * No commands specified to get the count, so the response is empty
     */
    @Override
    public long getCount(CommandOutput<?, ?> response) throws RecordCountException
    {
        throw new UnsupportedOperationException("Responsys data does not supporting getting counts");
    }

    /**
     * Gets the next file to pull and creates the corresponding SftpCommands
     * It first executes any UNIX commands in SOURCE_DATA_COMMANDS in order to 
     * move into the directory where the data exists. It downloads the data to a
     * temp folder, after which is will be decrypted
     */
    @Override
    public List<Command> getDataMetadata(String schema, String entity, WorkUnit workUnit, List<Predicate> predicateList) throws DataRecordException
    {
        if (this.filesToPull.hasNext()) {
            this.currentFile = this.filesToPull.next();
            this.filesToPull.remove();
        } else {
            return new ArrayList<Command>();
        }
        
        log.info("Current file to process: " + this.currentFile);
        
        List<Command> cmds = new ArrayList<Command>();
        List<String> getParams = new ArrayList<String>();
        getParams.add(this.currentFile);
        cmds.add(new SftpCommand().build(getParams, SftpCommandType.GET_STREAM));
        return cmds;
    }

    /**
     * Reads the data from the temp folder, and then decrypts the data
     * using the GPG protocol. The decryption returns an input stream
     * to the data, which is then converted to an iterator
     */
    @Override
    public Iterator<String> getData(CommandOutput<?, ?> response) throws DataRecordException, IOException
    {
        if (response.getResults().size() != 1) {
            throw new DataRecordException("Response has more than one command output entry");
        }
        
        this.sftpInputStream = (InputStream) response.getResults().values().iterator().next();
        this.decryptedInputStream = GPGFileDecrypter.decryptGPGFile(sftpInputStream, this.workUnit.getProp(RESPONSYS_DECRYPT_KEY));        
        Iterator<String> dataItr = IOUtils.lineIterator(decryptedInputStream, "UTF-8");
        
        if (this.workUnit.getPropAsBoolean(ConfigurationKeys.SOURCE_SKIP_FIRST_RECORD, false) && dataItr.hasNext()) {
            dataItr.next();
        }
        return dataItr;
    }

    /**
     * No-op because the schema is taken from the config
     */
    @Override
    public Map<String, String> getDataTypeMap() {
        return new HashMap<String, String>();
    }

    /**
     * No-op because there is no source specific API for Responsys
     */
    @Override
    public Iterator<String> getRecordSetFromSourceApi(String schema, String entity, WorkUnit workUnit, List<Predicate> predicateList) throws IOException
    {
        return null;
    }

    /**
     * No-op since not using Watermark
     */
    @Override
    public String getWatermarkSourceFormat(WatermarkType watermarkType)
    {
        return "";
    }

    /**
     * No-op since not using Watermark
     */
    @Override
    public String getHourPredicateCondition(String column, long value, String valueFormat, String operator)
    {
        return "";
    }

    /**
     * No-op since not using Watermark
     */
    @Override
    public String getDatePredicateCondition(String column, long value, String valueFormat, String operator)
    {
        return "";
    }

    /**
     * No-op since not using Watermark
     */
    @Override
    public String getTimestampPredicateCondition(String column, long value, String valueFormat, String operator)
    {
        return "";
    }

    @Override
    public void setTimeOut(String timeOut)
    {
    }
    
    /**
     * Close input streams
     */
    @Override
    public void closeConnection() throws Exception
    {
        super.closeConnection();
        IOUtils.closeQuietly(decryptedInputStream);
        sftpInputStream.close();
    }
}