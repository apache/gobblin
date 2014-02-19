package com.linkedin.uif.qualitychecker;

import com.linkedin.uif.configuration.ConfigurationKeys;
import com.linkedin.uif.configuration.MetaStoreClient;
import com.linkedin.uif.scheduler.TaskState;

public class RowCountRangePolicy extends Policy
{
    private long rowsRead;
    private long rowsWritten;
    private double range;
    
    public RowCountRangePolicy(TaskState taskState, MetaStoreClient metadata, Type type)
    {
        super(taskState, metadata, type);
        this.rowsRead = taskState.getPropAsLong(ConfigurationKeys.EXTRACTOR_ROWS_READ);
        this.rowsWritten = taskState.getPropAsLong(ConfigurationKeys.WRITER_ROWS_WRITTEN);
        this.range = taskState.getPropAsDouble(ConfigurationKeys.ROW_COUNT_RANGE);
    }

    @Override
    public PolicyResult executePolicy() {
        if (Math.abs((this.rowsWritten - this.rowsRead) / this.rowsRead) <= this.range) {
            return PolicyResult.PASSED;
        } else {
            return PolicyResult.FAILED;
        }
    }
}
