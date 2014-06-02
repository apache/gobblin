package com.linkedin.uif.source.extractor.extract.restapi;

import java.util.HashMap;
import java.util.Map;

import com.linkedin.uif.source.extractor.extract.CommandOutput;

public class RestApiCommandOutput implements CommandOutput<RestApiCommand, String>
{

    private Map<RestApiCommand, String> results = new HashMap<RestApiCommand, String>();
    
    @Override
    public void storeResults(Map<RestApiCommand, String> results)
    {
        this.results = results;
    }

    @Override
    public Map<RestApiCommand, String> getResults()
    {
        return results;
    }

    @Override
    public void put(RestApiCommand key, String value)
    {
        results.put(key, value);
    }
}
