package com.linkedin.uif.source.extractor.extract.sftp;

import com.linkedin.uif.configuration.WorkUnitState;
import com.linkedin.uif.source.extractor.filebased.FileBasedExtractor;

/**
 * Abstract class that implements the SFTP
 * protocol for connecting to source
 * and downloading files
 * @author stakiar
 */
public class SftpExtractor<S, D> extends FileBasedExtractor<S, D>
{
    public SftpExtractor(WorkUnitState workUnitState)
    {
        super(workUnitState, new SftpFsHelper(workUnitState));
    }
}
