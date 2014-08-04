 package com.linkedin.uif.source.extractor.extract.sftp;

import java.io.IOException;

import com.linkedin.uif.configuration.State;
import com.linkedin.uif.configuration.WorkUnitState;
import com.linkedin.uif.source.extractor.Extractor;
import com.linkedin.uif.source.extractor.filebased.FileBasedHelperException;
import com.linkedin.uif.source.extractor.filebased.FileBasedSource;

public class SftpSource<S, D> extends FileBasedSource<S, D>
{
    @Override
    public Extractor<S, D> getExtractor(WorkUnitState state) throws IOException
    {
        return new SftpExtractor<S, D>(state);
    }

    @Override
    public void initFileSystemHelper(State state) throws FileBasedHelperException
    {
        this.fsHelper = new SftpFsHelper(state);
        this.fsHelper.connect();
    }
}
