package com.linkedin.uif.policies.schema;

import com.linkedin.uif.configuration.State;
import com.linkedin.uif.qualitychecker.row.RowLevelPolicy;

public class SchemaRowCheckPolicy extends RowLevelPolicy
{

    public SchemaRowCheckPolicy(State state, Type type)
    {
        super(state, type);
    }

    @Override
    public Result executePolicy(Object record)
    {
        return RowLevelPolicy.Result.PASSED;
    }
}
