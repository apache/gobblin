package com.linkedin.uif.publisher;

import java.lang.reflect.Constructor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.linkedin.uif.configuration.ConfigurationKeys;
import com.linkedin.uif.configuration.MetaStoreClient;
import com.linkedin.uif.configuration.WorkUnitState;
import com.linkedin.uif.qualitychecker.PolicyCheckResults;

public class TaskPublisherBuilder
{
    private final MetaStoreClient metadata;
    private final PolicyCheckResults results;
    private final WorkUnitState workUnitState;
    
    private static final Log LOG = LogFactory.getLog(TaskPublisherBuilder.class);
    
    public TaskPublisherBuilder(WorkUnitState workUnitState, PolicyCheckResults results, MetaStoreClient metadata) {
        this.metadata = metadata;
        this.results = results;
        this.workUnitState = workUnitState;
    }
    
    public static TaskPublisherBuilder newBuilder(WorkUnitState taskState, PolicyCheckResults results, MetaStoreClient metadata, DataPublisher dataPublisher) {
        return new TaskPublisherBuilder(taskState, results, metadata);
    }
    
    @SuppressWarnings("unchecked")
    private DataPublisher createDataPublisher() throws Exception {
        DataPublisher dataPublisher;
        String dataPublisherString = this.workUnitState.getProp(ConfigurationKeys.QUALITY_CHECKER_PREFIX + ConfigurationKeys.TASK_DATA_PUBLISHER_TYPE);
        try {
            Class<? extends DataPublisher> dataPublisherClass = (Class<? extends DataPublisher>) Class.forName(dataPublisherString);
            Constructor<? extends DataPublisher> dataPublisherConstructor = dataPublisherClass.getConstructor(DataPublisher.class);
            dataPublisher = dataPublisherConstructor.newInstance(this.workUnitState);
        } catch (Exception e) {
            LOG.error("");
            throw e;
        }
        return dataPublisher;
    }
    
    public TaskPublisher build() throws Exception {
        return new TaskPublisher(this.workUnitState, this.results, this.metadata, createDataPublisher());
    }
}
