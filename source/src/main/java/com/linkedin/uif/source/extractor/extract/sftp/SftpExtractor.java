package com.linkedin.uif.source.extractor.extract.sftp;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.Iterator;
import java.util.List;

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
import com.linkedin.uif.source.extractor.extract.QueryBasedExtractor;
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
public abstract class SftpExtractor extends QueryBasedExtractor<String, String> implements SourceSpecificLayer<String, String>
{
    private static final Logger log = LoggerFactory.getLogger(SftpExtractor.class);
    
    private ChannelSftp sftp;
    private SftpExecutor executor;
    
    public SftpExtractor(WorkUnitState workUnitState)
    {
        super(workUnitState);
        this.sftp = createSftpConnection();
        this.executor = new SftpExecutor();
    }
    
    /**
     * Initializes the ChannelSftp
     * @return the ChannelSftp connection
     */
    public ChannelSftp createSftpConnection() {
        ChannelSftp sftp = (ChannelSftp) SftpExecutor.connect(this.workUnitState.getProp(ConfigurationKeys.SOURCE_CONN_PRIVATE_KEY),
                                                              this.workUnitState.getProp(ConfigurationKeys.SOURCE_CONN_KNOWN_HOSTS),
                                                              this.workUnitState.getProp(ConfigurationKeys.SOURCE_CONN_USERNAME),
                                                              this.workUnitState.getProp(ConfigurationKeys.SOURCE_CONN_HOST_NAME),
                                                              this.workUnitState.getProp(ConfigurationKeys.SOURCE_CONN_USE_PROXY_URL),
                                                              this.workUnitState.getPropAsInt(ConfigurationKeys.SOURCE_CONN_USE_PROXY_PORT, -1));
        return sftp;
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
            CommandOutput<SftpCommand, List<String>> response = SftpExecutor.executeUnixCommands(cmds, this.sftp);
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
            CommandOutput<SftpCommand, List<String>> response = SftpExecutor.executeUnixCommands(cmds, this.sftp);
            String array = this.getSchema(response);
            this.setOutputSchema(array);
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
    public Iterator<String> getRecordSet(String schema, String entity, WorkUnit workUnit, List<Predicate> predicateList) throws DataRecordException, IOException
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
                        executor.executeGetFileCommand(sftpCmd, this.sftp, this.workUnit.getProp(ConfigurationKeys.WRITER_FILE_SYSTEM_URI));
                    } else if (sftpCmd.getCommandType().equals(SftpCommandType.GET_STREAM)) {
                        InputStream stream = executor.executeGetStreamCommand(sftpCmd, this.sftp);
                        CommandOutput<SftpCommand, InputStream> getStreamOutput = new SftpGetCommandOuput();
                        getStreamOutput.put(sftpCmd, stream);
                        return this.getData(getStreamOutput);
                    } else {
                        SftpExecutor.executeUnixCommand(sftpCmd, this.sftp);
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
        catch (URISyntaxException e)
        {
            throw new IOException(e.getMessage(), e);
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
        log.info("Shutting down the sftp connection");
        this.sftp.disconnect();
    }
}
