package com.linkedin.uif.source.extractor.extract.sftp;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import com.linkedin.uif.source.extractor.extract.CommandOutput;

public class SftpGetCommandOuput implements CommandOutput<SftpCommand, InputStream>
{
    private Map<SftpCommand, InputStream> results;
    
    public SftpGetCommandOuput() {
        results = new HashMap<SftpCommand, InputStream>();
    }
    
    @Override
    public void storeResults(Map<SftpCommand, InputStream> results)
    {
        this.results = results;
    }

    @Override
    public Map<SftpCommand, InputStream> getResults()
    {
        return this.results;
    }

    @Override
    public void put(SftpCommand key, InputStream value)
    {
        this.results.put(key, value);
    }
}
