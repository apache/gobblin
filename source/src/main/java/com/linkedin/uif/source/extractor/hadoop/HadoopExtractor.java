package com.linkedin.uif.source.extractor.hadoop;

import java.io.IOException;
import java.util.Iterator;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;
import com.linkedin.uif.configuration.WorkUnitState;
import com.linkedin.uif.source.extractor.filebased.FileBasedExtractor;
import com.linkedin.uif.source.extractor.filebased.FileBasedHelperException;

public class HadoopExtractor extends FileBasedExtractor<Schema, GenericRecord>
{
    private Logger log = LoggerFactory.getLogger(HadoopExtractor.class);

    public HadoopExtractor(WorkUnitState workUnitState)
    {
        super(workUnitState, new HadoopFsHelper(workUnitState));
    }

    @Override
    public Iterator<GenericRecord> downloadFile(String file) throws IOException
    {
        log.info("About to download file " + file);
        
        DataFileReader<GenericRecord> dfr = null;
        try {
            dfr = ((HadoopFsHelper) this.fsHelper).getAvroFile(file);
            fileHandles.put(file, dfr);
            return dfr;
        } catch (FileBasedHelperException e) {
            Throwables.propagate(e);
        }
        return null;
    }
    
    /**
     * Assumption is that all files in the input directory have the same schema
     */
    @Override
    public Schema getSchema()
    {
        HadoopFsHelper hfsHelper = (HadoopFsHelper) this.fsHelper;
        if (this.filesToPull.isEmpty()) {
            return null;
        } else {
            try {
                return hfsHelper.getAvroSchema(this.filesToPull.get(0));
            } catch (FileBasedHelperException e) {
                Throwables.propagate(e);
                return null;
            }
        }
    }
}
