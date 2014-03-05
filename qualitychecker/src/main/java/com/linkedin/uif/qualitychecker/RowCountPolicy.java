package com.linkedin.uif.qualitychecker;

import com.linkedin.uif.configuration.ConfigurationKeys;
import com.linkedin.uif.configuration.State;

public class RowCountPolicy extends Policy
{
    private final long rowsRead;
    private final long rowsWritten;
    
    public RowCountPolicy(State state, Policy.Type type)
    {
        super(state, type);
        this.rowsRead = state.getPropAsLong(ConfigurationKeys.QUALITY_CHECKER_PREFIX + ConfigurationKeys.EXTRACTOR_ROWS_READ);
        this.rowsWritten = state.getPropAsLong(ConfigurationKeys.QUALITY_CHECKER_PREFIX + ConfigurationKeys.WRITER_ROWS_WRITTEN);
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
