package com.linkedin.uif.source.extractor.hadoop;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import com.linkedin.uif.configuration.State;
import com.linkedin.uif.configuration.WorkUnitState;
import com.linkedin.uif.source.extractor.Extractor;
import com.linkedin.uif.source.extractor.filebased.FileBasedHelperException;
import com.linkedin.uif.source.extractor.filebased.FileBasedSource;

public class HadoopSource extends FileBasedSource<Schema, GenericRecord>
{
    @Override
    public Extractor<Schema, GenericRecord> getExtractor(WorkUnitState state) throws IOException
    {
        return new HadoopExtractor(state);
    }

    @Override
    public void initFileSystemHelper(State state) throws FileBasedHelperException
    {
        this.fsHelper = new HadoopFsHelper(state);
        this.fsHelper.connect();
    }
}
