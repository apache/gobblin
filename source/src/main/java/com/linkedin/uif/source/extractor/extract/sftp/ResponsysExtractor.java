package com.linkedin.uif.source.extractor.extract.sftp;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jcraft.jsch.SftpException;
import com.linkedin.uif.configuration.ConfigurationKeys;
import com.linkedin.uif.configuration.WorkUnitState;
import com.linkedin.uif.source.extractor.extract.Command;
import com.linkedin.uif.source.extractor.extract.CommandOutput;
import com.linkedin.uif.source.extractor.extract.sftp.SftpCommand.SftpCommandType;

/**
 * Extractor to pull Responsys data using the SFTP protocol
 * @author stakiar
 *
 * @param <D> type of data record
 * @param <S> type of schema
 */
public class ResponsysExtractor extends SftpExtractor<String, String>
{
    private static final Logger log = LoggerFactory.getLogger(ResponsysExtractor.class);
    
    private static final String RESPONSYS_SCHEMA = "responsys.schema";
    private static final String RESPONSYS_DECRYPT_KEY = "responsys.decrypt.key.location";
    
    private Map<String, LineIterator> fileHandles;
    
    public ResponsysExtractor(WorkUnitState workUnitState)
    {
        super(workUnitState);
        this.fileHandles = new HashMap<String, LineIterator>();
    }
    
    @Override
    protected Iterator<String> downloadFile(String file) throws IOException
    {
        log.info("Beginning to download file: " + file);
        Command sftpCmd = new SftpCommand().build(Arrays.asList(file), SftpCommandType.GET_STREAM);

        try {
            Iterator<String> fileItr = IOUtils.lineIterator(GPGFileDecrypter.decryptGPGFile(executor.executeGetStreamCommand((SftpCommand) sftpCmd, this.sftp), this.workUnit.getProp(RESPONSYS_DECRYPT_KEY)), "UTF-8");
            fileHandles.put(file, (LineIterator) fileItr);
            if (workUnitState.getPropAsBoolean(ConfigurationKeys.SOURCE_SKIP_FIRST_RECORD, false) && fileItr.hasNext()) {
                fileItr.next();
            }
            return fileItr;
        } catch (SftpException e) {
            throw new IOException("Exception while downloading file " + file + " with message " + e.getMessage(), e);
        } catch (SftpCommandFormatException e) {
            throw new RuntimeException("Error while constructing download file commands " + e.getMessage(), e);
        }
    }
    
    @Override
    protected void closeFile(String file)
    {
        fileHandles.get(file).close();
    }

    @Override
    protected List<Command> getSchemaCommands()
    {
        return new ArrayList<Command>();
    }

    @Override
    protected String getSchemaFromOutput(CommandOutput<SftpCommand, List<String>> output)
    {
        return this.workUnit.getProp(RESPONSYS_SCHEMA);
    }

    @Override
    protected List<Command> getCountCommands()
    {
        return new ArrayList<Command>();
    }

    @Override
    protected long getCountFromOutput(CommandOutput<SftpCommand, List<String>> output)
    {
        return -1;
    }
}