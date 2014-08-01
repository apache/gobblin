package com.linkedin.uif.source.extractor.datapurger;

import com.linkedin.uif.configuration.WorkUnitState;
import com.linkedin.uif.source.extractor.hadoop.HadoopExtractor;

public class DataPurgerExtractor extends HadoopExtractor {

    public DataPurgerExtractor(WorkUnitState workUnitState)
    {
        super(workUnitState);
    } 
}
