package com.linkedin.uif.source.extractor.responsys;

import java.io.IOException;

import com.linkedin.uif.configuration.WorkUnitState;
import com.linkedin.uif.source.extractor.Extractor;
import com.linkedin.uif.source.extractor.extract.sftp.SftpSource;

/**
 * Source class for Responsys data, responsible for querying Responsys
 * in order to get a list of files to pull for this current run. It then
 * distributes the files among the work units
 * @author stakiar
 */
public class ResponsysSource extends SftpSource<String, String>
{  
    @Override
    public Extractor<String, String> getExtractor(WorkUnitState state) throws IOException
    {
        return new ResponsysExtractor(state);
    }
}
