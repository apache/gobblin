package com.linkedin.uif.source.extractor.extract.sftp;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.io.IOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.SftpException;

import com.linkedin.uif.configuration.ConfigurationKeys;
import com.linkedin.uif.configuration.WorkUnitState;
import com.linkedin.uif.source.extractor.DataRecordException;
import com.linkedin.uif.source.extractor.exception.HighWatermarkException;
import com.linkedin.uif.source.extractor.exception.RecordCountException;
import com.linkedin.uif.source.extractor.exception.SchemaException;
import com.linkedin.uif.source.extractor.extract.BaseExtractor;
import com.linkedin.uif.source.extractor.extract.Command;
import com.linkedin.uif.source.extractor.extract.CommandOutput;
import com.linkedin.uif.source.extractor.extract.SourceSpecificLayer;
import com.linkedin.uif.source.extractor.extract.sftp.SftpCommand.SftpCommandType;
import com.linkedin.uif.source.extractor.watermark.Predicate;
import com.linkedin.uif.source.workunit.WorkUnit;

/**
 * Abstract class that implements the SFTP
 * protocol for connecting to source
 * and downloading files
 * @author stakiar
 *
 * @param <D> type of data record
 * @param <S> type of schema
 */
public abstract class SftpExtractor<S, D> extends BaseExtractor<S, D> implements SourceSpecificLayer<S, D>
{
    private static final Logger log = LoggerFactory.getLogger(SftpExtractor.class);
    
    private ChannelSftp sftp;
    private SftpExecutor executor;
    
    public SftpExtractor(WorkUnitState workUnitState)
    {
        super(workUnitState);
        this.executor = new SftpExecutor();
    }
    
    /**
     * Initializes the ChannelSftp
     * if it hasn't been created
     * otherwise it just returns it
     * @return the ChannelSftp connection
     */
    public ChannelSftp getSftp() {
        if (sftp == null) {
            sftp = (ChannelSftp) SftpExecutor.connect(this.workUnit.getProp(ConfigurationKeys.SOURCE_PRIVATE_KEY),
                                                      this.workUnit.getProp(ConfigurationKeys.SOURCE_KNOWN_HOSTS),
                                                      this.workUnit.getProp(ConfigurationKeys.SOURCE_USERNAME),
                                                      this.workUnit.getProp(ConfigurationKeys.SOURCE_HOST_NAME),
                                                      this.workUnit.getProp(ConfigurationKeys.SOURCE_USE_PROXY_URL),
                                                      this.workUnit.getPropAsInt(ConfigurationKeys.SOURCE_USE_PROXY_PORT, -1));
        }
        return this.sftp;
    }
     
    /**
     * Gets the max watermark
     * @return the max watermark
     */
    @Override
    public long getMaxWatermark(String schema, String entity, String watermarkColumn, List<Predicate> snapshotPredicateList, String watermarkSourceFormat) throws HighWatermarkException
    {
        log.info("Getting max watermark");
        List<Command> cmds = this.getHighWatermarkMetadata(schema, entity, watermarkColumn, snapshotPredicateList);
        try {
            CommandOutput<SftpCommand, List<String>> response = SftpExecutor.executeUnixCommands(cmds, getSftp());
            return this.getHighWatermark(response, watermarkColumn, watermarkSourceFormat);
        }
        catch (SftpException e)
        {
            throw new HighWatermarkException("SftpException while getting maxWatermark: " + e.getMessage(), e);
        }
        catch (SftpCommandFormatException e)
        {
            throw new HighWatermarkException("SftpCommandFormatException whilte getting maxWatermark: " + e.getMessage(), e);
        }  
    }

    /**
     * Gets the metadata for this job (e.g. the schema)
     */
    @Override
    public void extractMetadata(String schema, String entity, WorkUnit workUnit) throws SchemaException, IOException
    {
        log.info("Getting the schema");
        List<Command> cmds = this.getSchemaMetadata(schema, entity);
        try
        {
            CommandOutput<SftpCommand, List<String>> response = SftpExecutor.executeUnixCommands(cmds, getSftp());
            S array = this.getSchema(response);
            this.setOutputSchema((S) array);
        }
        catch (SftpException e)
        {
            throw new SchemaException("SftpException while getting schema: " + e.getMessage(), e);
        }
        catch (SftpCommandFormatException e)
        {
            throw new SchemaException("SftpCommandFormatException while getting schema: " + e.getMessage(), e);
        }        
    }
    
    /**
     * Downloads the file from the source and returns it as an iterator
     */
    @SuppressWarnings("unchecked")
    @Override
    public Iterator<D> getRecordSet(String schema, String entity, WorkUnit workUnit, List<Predicate> predicateList) throws DataRecordException, IOException
    {
        try
        {
            log.info("Fetching record set");
            List<Command> cmds = this.getDataMetadata(schema, entity, workUnit, predicateList);
            if (cmds == null || cmds.isEmpty()) {
                return null;
            }
            for (Command cmd : cmds) {
                if (cmd instanceof SftpCommand) {
                    SftpCommand sftpCmd = (SftpCommand) cmd;
                    if (sftpCmd.getCommandType().equals(SftpCommandType.GET_FILE)) {
                        executor.executeGetFileCommand(sftpCmd, this.getSftp());
                    } else if (sftpCmd.getCommandType().equals(SftpCommandType.GET_STREAM)) {
                        InputStream stream = executor.executeGetStreamCommand(sftpCmd, this.getSftp());
                        return (Iterator<D>) IOUtils.lineIterator(stream, "UTF-8");
                    } else {
                        SftpExecutor.executeUnixCommand(sftpCmd, getSftp());
                    }
                } else {
                    throw new DataRecordException("Illegal command given to getRecordSet()");
                }
            }
            return this.getData(new SftpCommandOutput());
        }
        catch (SftpException e)
        {
            throw new DataRecordException(e.getMessage(),  e);
        }
        catch (SftpCommandFormatException e)
        {
            throw new DataRecordException(e.getMessage(), e);
        }
    }
    
    /**
     * Gets the count of the files
     * 
     * TODO this can be implemented using the
     * ChannelSftp.stat(path) command (not
     * applicable for Responsys since the files
     * on the server are encrypted)
     * 
     * TODO this only supports downloading
     * of not file, not multiple files
     * 
     * @return the count of the files to download
     */
    @Override
    public long getSourceCount(String schema,
                               String entity,
                               WorkUnit workUnit,
                               List<Predicate> predicateList) throws RecordCountException
    {
        return -1;
    }
    
    /**
     * Close ChannelSftp connection
     */
    @Override
    public void closeConnection() throws Exception
    {
        this.getSftp().disconnect();
    }
}
