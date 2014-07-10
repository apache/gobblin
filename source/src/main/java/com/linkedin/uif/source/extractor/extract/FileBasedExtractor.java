package com.linkedin.uif.source.extractor.extract;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;
import com.jcraft.jsch.SftpException;
import com.linkedin.uif.configuration.ConfigurationKeys;
import com.linkedin.uif.configuration.WorkUnitState;
import com.linkedin.uif.source.extractor.DataRecordException;
import com.linkedin.uif.source.extractor.Extractor;
import com.linkedin.uif.source.extractor.extract.QueryBasedExtractor;
import com.linkedin.uif.source.extractor.extract.Command;
import com.linkedin.uif.source.extractor.extract.CommandOutput;
import com.linkedin.uif.source.extractor.extract.sftp.SftpCommand;
import com.linkedin.uif.source.extractor.extract.sftp.SftpCommandFormatException;
import com.linkedin.uif.source.extractor.extract.sftp.SftpCommand.SftpCommandType;
import com.linkedin.uif.source.workunit.WorkUnit;

/**
 * Abstract class for file based extractors
 * @author stakiar
 *
 * @param <S> type of schema
 * @param <D> type of data record
 * @param <K> key type of the command output 
 * @param <V> value type of the command output
 */
public abstract class FileBasedExtractor<S, D, K extends Command, V> implements Extractor<S, D>
{
    protected WorkUnitState workUnitState;
    protected WorkUnit workUnit;
    
    protected List<String> filesToPull;
    private Iterator<D> currentFileItr;
    private String currentFile;
    private boolean readRecordStart;
    
    private Logger log = LoggerFactory.getLogger(FileBasedExtractor.class);

    public FileBasedExtractor(WorkUnitState workUnitState) {
        this.workUnitState = workUnitState;
        this.workUnit = workUnitState.getWorkunit();
        this.filesToPull = new ArrayList<String>(workUnitState.getPropAsList(ConfigurationKeys.SOURCE_FILEBASED_FILES_TO_PULL, ""));
    }
    
    /**
     * Get a list of commands to execute on the source file system,
     * executes the commands, and parses the output for the schema
     * @return the schema
     */
    @Override
    public S getSchema()
    {
        log.info("Getting schema");
        List<Command> cmds = getSchemaCommands();
        CommandOutput<K, V> output = executeCommands(cmds);
        return getSchemaFromOutput(output);
    }    

    /**
     * Gets a list of file system commands to fetch the schema
     * @return a list of commands to execute
     */
    protected abstract List<Command> getSchemaCommands();
    
    /**
     * Parses a command output and returns the schema
     * @param output the output of the schema commands
     * @return the schema
     */
    protected abstract S getSchemaFromOutput(CommandOutput<K, V> output);

    /**
     * Initializes a list of files to pull on the first call to the method
     * Iterates through the file and returns a new record upon each call
     * until there are no more records left in the file, then it moves on
     * to the next file
     */
    @Override
    public D readRecord() throws DataRecordException, IOException
    {   
        if (!readRecordStart) {
            log.info("Starting to read records");
            if (!filesToPull.isEmpty()) {
                currentFile = filesToPull.remove(0);
                currentFileItr = downloadFile(currentFile);
            } else {
                log.info("Finished reading records");
                return null;
            }
            readRecordStart = true;
        }
        
        while (!currentFileItr.hasNext() && !filesToPull.isEmpty()) {
            closeFile(currentFile);
            currentFile = filesToPull.remove(0);
            currentFileItr = downloadFile(currentFile);
        }
        
        if (currentFileItr.hasNext()) {
            return (D) currentFileItr.next();
        } else {
            log.info("Finished reading records");
            return null;
        }
    }
    
    /**
     * Downloads a file from the source
     * @param f is the file to download
     * @return an iterator over the file
     */
    protected abstract Iterator<D> downloadFile(String file) throws IOException;
    
    /**
     * Closes a file from the source
     * @param f is the file to download
     * @return an iterator over the file
     */
    protected abstract void closeFile(String file);
    
    /**
     * Gets a list of commands that will get the
     * expected record count from the source, executes the commands,
     * and then parses the output for the count
     * @return the expected record count
     */
    @Override
    public long getExpectedRecordCount()
    {
        log.info("Getting record count");
        List<Command> cmds = getCountCommands();
        CommandOutput<K, V> output = executeCommands(cmds);
        return getCountFromOutput(output);
    }

    /**
     * Gets a list of file system commands to get the
     * expected record count
     * @return a list of commands to execute
     */
    protected abstract List<Command> getCountCommands();
    
    /**
     * Parses a command output and gets the expected
     * record count
     * @param output is the output from the commands
     * @return the expected record count
     */
    protected abstract long getCountFromOutput(CommandOutput<K, V> output);

    /**
     * Gets a list of commands that will get the
     * high watermark from the source, executes the commands,
     * and then parses the output for the watermark
     * @return the high watermark
     */
    @Override
    public long getHighWatermark()
    {
        log.info("High Watermark is -1 for file based extractors");
        return -1;
    }
    
    /**
     * Executes a given list of protocol specific commands
     * @param cmds is a list of commands to execute
     * @return the output of each command
     */
    public abstract CommandOutput<K, V> executeCommands(List<Command> cmds);
    
    public WorkUnitState getWorkUnitState() {
        return this.workUnitState;
    }
    
    public WorkUnit getWorkUnit() {
        return this.workUnit;
    }
}