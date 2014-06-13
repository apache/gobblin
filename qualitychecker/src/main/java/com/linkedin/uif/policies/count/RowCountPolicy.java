package com.linkedin.uif.policies.count;

import com.linkedin.uif.configuration.ConfigurationKeys;
import com.linkedin.uif.configuration.State;
import com.linkedin.uif.qualitychecker.task.TaskLevelPolicy;

public class RowCountPolicy extends TaskLevelPolicy
{
    private final long rowsRead;
    private final long rowsWritten;

    public RowCountPolicy(State state, TaskLevelPolicy.Type type)
    {
        super(state, type);
        this.rowsRead = state.getPropAsLong(ConfigurationKeys.EXTRACTOR_ROWS_EXPECTED);
        this.rowsWritten = state.getPropAsLong(ConfigurationKeys.WRITER_ROWS_WRITTEN);
    }

    @Override
    public Result executePolicy() {
        if (this.rowsRead == this.rowsWritten) {
            return Result.PASSED;
        } else {
            return Result.FAILED;
        }
    }
}
