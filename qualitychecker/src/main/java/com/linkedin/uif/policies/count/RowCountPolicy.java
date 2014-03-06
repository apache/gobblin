package com.linkedin.uif.policies.count;

import com.linkedin.uif.configuration.ConfigurationKeys;
import com.linkedin.uif.configuration.State;
import com.linkedin.uif.qualitychecker.Policy;
import com.linkedin.uif.qualitychecker.Policy.Result;
import com.linkedin.uif.qualitychecker.Policy.Type;

public class RowCountPolicy extends Policy
{
    private final long rowsRead;
    private final long rowsWritten;
    
    public RowCountPolicy(State state, Policy.Type type)
    {
        super(state, type);
        this.rowsRead = state.getPropAsLong(ConfigurationKeys.EXTRACTOR_ROWS_READ);
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
