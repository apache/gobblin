package com.linkedin.uif.source.extractor.extract.sftp;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.uif.configuration.WorkUnitState;
import com.linkedin.uif.source.extractor.DataRecordException;
import com.linkedin.uif.source.extractor.Extractor;
import com.linkedin.uif.source.extractor.extract.QueryBasedExtractor;
import com.linkedin.uif.source.extractor.extract.Command;
import com.linkedin.uif.source.extractor.extract.CommandOutput;
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
    private WorkUnitState workUnitState;
    private WorkUnit workUnit;
    
    private List<File> filesToPull;
    private Iterator<D> currentFileItr;
    private boolean readRecordStart;
    
    private Logger log = LoggerFactory.getLogger(QueryBasedExtractor.class);

    public FileBasedExtractor(WorkUnitState workUnitState) {
        this.workUnitState = workUnitState;
        this.workUnit = workUnitState.getWorkunit();
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
     * @return a data record
     */
    @Override
    public D readRecord() throws DataRecordException, IOException
    {   
        if (!readRecordStart) {
            log.info("Starting to read records");
            filesToPull = getListOfFiles();
            if (!filesToPull.isEmpty()) {
                currentFileItr = downloadFile(filesToPull.remove(0));
            } else {
                log.info("Finished reading records");
                return null;
            }
            readRecordStart = true;
        }
        
        while (!currentFileItr.hasNext() && !filesToPull.isEmpty()){
            currentFileItr = downloadFile(filesToPull.remove(0));
        }
        
        if (currentFileItr.hasNext()) {
            return currentFileItr.next();
        } else {
            log.info("Finished reading records");
            return null;
        }
    }

    /**
     * Gets a list of files to pull
     * @return a list of files to pull
     */
    protected abstract List<File> getListOfFiles();
    
    /**
     * Downloads a file from the source
     * @param f is the file to download
     * @return an iterator over the file
     */
    protected abstract Iterator<D> downloadFile(File f);
    
    /**
     * Closes the source connection and protocol connection
     */
    @Override
    public void close()
    {
        log.info("Closing extractor");
        closeSource();
        closeProtocol();
    }

    /**
     * Closes the source connection
     */
    protected abstract void closeSource();

    /**
     * Closes the protocol connection
     */
    protected abstract void closeProtocol();
    
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
        log.info("Getting high watermark");
        List<Command> cmds = getWatermarkCommands();
        CommandOutput<K, V> output = executeCommands(cmds);
        return getWatermarkFromOutput(output);
    }

    /**
     * Gets a list of file system commands to get the
     * high watermark
     * @return a list of commands to execute
     */
    protected abstract List<Command> getWatermarkCommands();
    
    /**
     * Parses a command output and gets high watermark
     * @param output is the output from the commands
     * @return the high watermark
     */
    protected abstract long getWatermarkFromOutput(CommandOutput<K, V> output);
    
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