package com.linkedin.uif.source.extractor.hadoop;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import com.linkedin.uif.configuration.State;
import com.linkedin.uif.configuration.WorkUnitState;
import com.linkedin.uif.source.extractor.Extractor;
import com.linkedin.uif.source.extractor.filebased.FileBasedSource;
import com.linkedin.uif.source.extractor.filebased.FileBasedHelper;

public class HadoopSource extends FileBasedSource<Schema, GenericRecord>
{
    @Override
    public Extractor<Schema, GenericRecord> getExtractor(WorkUnitState state) throws IOException
    {
        return new HadoopExtractor(state);
    }

    @Override
    public FileBasedHelper initFileSystemHelper(State state)
    {
        return new HadoopFsHelper(state);
    }
}
