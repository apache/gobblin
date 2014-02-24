package com.linkedin.uif.qualitychecker;

import com.linkedin.uif.configuration.ConfigurationKeys;
import com.linkedin.uif.configuration.MetaStoreClient;
import com.linkedin.uif.scheduler.TaskState;

public class RowCountPolicy extends Policy
{
    private final long rowsRead;
    private final long rowsWritten;
    
    public RowCountPolicy(TaskState taskState, MetaStoreClient metadata, Type type)
    {
        super(taskState, metadata, type);
        this.rowsRead = taskState.getPropAsLong(ConfigurationKeys.EXTRACTOR_ROWS_READ);
        this.rowsWritten = taskState.getPropAsLong(ConfigurationKeys.WRITER_ROWS_WRITTEN);
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
