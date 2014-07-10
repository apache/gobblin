package com.linkedin.uif.source.extractor.extract;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import com.linkedin.uif.configuration.WorkUnitState;

public class DataPurgerExtractor extends FileSystemExtractor<Schema, GenericRecord, FileSystemCommand, List<String>>
{

    public DataPurgerExtractor(WorkUnitState workUnitState)
    {
        super(workUnitState);
    }

    @Override
    protected List<Command> getSchemaCommands()
    {
        FileSystemCommand cmd = new FileSystemCommand();
        return null;
    }

    @Override
    protected Schema getSchemaFromOutput(CommandOutput<FileSystemCommand, List<String>> output)
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    protected Iterator<GenericRecord> downloadFile(String file) throws IOException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    protected void closeFile(String file)
    {
        // TODO Auto-generated method stub
        
    }

    @Override
    protected List<Command> getCountCommands()
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    protected long getCountFromOutput(CommandOutput<FileSystemCommand, List<String>> output)
    {
        // TODO Auto-generated method stub
        return 0;
    }
}
