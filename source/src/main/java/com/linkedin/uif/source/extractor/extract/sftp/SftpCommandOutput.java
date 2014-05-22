package com.linkedin.uif.source.extractor.extract.sftp;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.linkedin.uif.source.extractor.extract.CommandOutput;

/**
 * Captures the output of an SFTP
 * command, uses a Map to keep track
 * of the command and a list of Strings
 * representing the output of that
 * command
 * @author stakiar
 */
public class SftpCommandOutput implements CommandOutput<SftpCommand, List<String>>
{
    private Map<SftpCommand, List<String>> results;
    
    public SftpCommandOutput() {
        results = new HashMap<SftpCommand, List<String>>();
    }
    
    @Override
    public void storeResults(Map<SftpCommand, List<String>> results)
    {
        this.results = results;
    }

    @Override
    public Map<SftpCommand, List<String>> getResults()
    {
        return results;
    }

    @Override
    public void put(SftpCommand key, List<String> value)
    {
        results.put(key, value);
    }
}