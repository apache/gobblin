package com.linkedin.uif.policies.count;

import com.linkedin.uif.configuration.ConfigurationKeys;
import com.linkedin.uif.configuration.State;
import com.linkedin.uif.qualitychecker.Policy;
import com.linkedin.uif.qualitychecker.Policy.Result;
import com.linkedin.uif.qualitychecker.Policy.Type;

public class RowCountRangePolicy extends Policy
{
    private final long rowsRead;
    private final long rowsWritten;
    private final double range;
    
    public RowCountRangePolicy(State state, Type type)
    {
        super(state, type);
        this.rowsRead = state.getPropAsLong(ConfigurationKeys.EXTRACTOR_ROWS_READ);
        this.rowsWritten = state.getPropAsLong(ConfigurationKeys.WRITER_ROWS_WRITTEN);
        this.range = state.getPropAsDouble(ConfigurationKeys.ROW_COUNT_RANGE);
    }

    @Override
    public Result executePolicy() {
        if (Math.abs((this.rowsWritten - this.rowsRead) / this.rowsRead) <= this.range) {
            return Result.PASSED;
        } else {
            return Result.FAILED;
        }
    }
}
