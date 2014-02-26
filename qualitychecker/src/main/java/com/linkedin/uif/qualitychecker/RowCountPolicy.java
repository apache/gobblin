package com.linkedin.uif.qualitychecker;

import com.linkedin.uif.configuration.ConfigurationKeys;
import com.linkedin.uif.configuration.MetaStoreClient;
import com.linkedin.uif.configuration.WorkUnitState;

public class RowCountPolicy extends Policy
{
    private final long rowsRead;
    private final long rowsWritten;
    
    public RowCountPolicy(WorkUnitState workUnitState, MetaStoreClient metadata, Type type)
    {
        super(workUnitState, metadata, type);
        this.rowsRead = workUnitState.getPropAsLong(ConfigurationKeys.EXTRACTOR_ROWS_READ);
        this.rowsWritten = workUnitState.getPropAsLong(ConfigurationKeys.WRITER_ROWS_WRITTEN);
    }

    @Override
    public QualityCheckResult executePolicy() {
        if (this.rowsRead == this.rowsWritten) {
            return QualityCheckResult.PASSED;
        } else {
            return QualityCheckResult.FAILED;
        }
    }
}
