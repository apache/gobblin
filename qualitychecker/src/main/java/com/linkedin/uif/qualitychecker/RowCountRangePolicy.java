package com.linkedin.uif.qualitychecker;

import com.linkedin.uif.configuration.ConfigurationKeys;
import com.linkedin.uif.configuration.MetaStoreClient;
import com.linkedin.uif.configuration.WorkUnitState;

public class RowCountRangePolicy extends Policy
{
    private final long rowsRead;
    private final long rowsWritten;
    private final double range;
    
    public RowCountRangePolicy(WorkUnitState workUnitState, MetaStoreClient metadata, Type type)
    {
        super(workUnitState, metadata, type);
        this.rowsRead = workUnitState.getPropAsLong(ConfigurationKeys.QUALITY_CHECKER_PREFIX + ConfigurationKeys.EXTRACTOR_ROWS_READ);
        this.rowsWritten = workUnitState.getPropAsLong(ConfigurationKeys.QUALITY_CHECKER_PREFIX + ConfigurationKeys.WRITER_ROWS_WRITTEN);
        this.range = workUnitState.getPropAsDouble(ConfigurationKeys.QUALITY_CHECKER_PREFIX + ConfigurationKeys.ROW_COUNT_RANGE);
    }

    @Override
    public QualityCheckResult executePolicy() {
        if (Math.abs((this.rowsWritten - this.rowsRead) / this.rowsRead) <= this.range) {
            return QualityCheckResult.PASSED;
        } else {
            return QualityCheckResult.FAILED;
        }
    }
}
