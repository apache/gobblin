package com.linkedin.uif.qualitychecker;

import java.lang.reflect.Constructor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.linkedin.uif.configuration.ConfigurationKeys;
import com.linkedin.uif.configuration.MetaStoreClient;
import com.linkedin.uif.configuration.WorkUnitState;
import com.linkedin.uif.publisher.TaskPublisher;
import com.linkedin.uif.scheduler.TaskState;

/**
 * Creates a PolicyChecker and initializes the PolicyList
 * the list is Policies to create is taken from the
 * MetadataCollector
 */
public class PolicyCheckerBuilder
{   
    private final MetaStoreClient metadata;
    private final TaskState taskState;
    
    private static final Log LOG = LogFactory.getLog(PolicyCheckerBuilder.class);
    
    public PolicyCheckerBuilder(TaskState taskState, MetaStoreClient metadata) {
        this.metadata = metadata;
        this.taskState = taskState;
    }
    
    @SuppressWarnings("unchecked")
    public PolicyList createPolicyList() throws Exception {
        PolicyList list = new PolicyList();
        String[] policies = this.taskState.getProp(ConfigurationKeys.POLICY_LIST).split(",");
        String[] types = this.taskState.getProp(ConfigurationKeys.POLICY_LIST_TYPE).split(",");
        if (policies.length != types.length ) throw new Exception("Policies list and Policies list type are not the same length");
        for (int i = 0; i < policies.length; i++) {
            try {
                Class<? extends Policy> policyClass = (Class<? extends Policy>) Class.forName(policies[i]);
                Constructor<? extends Policy> policyConstructor = policyClass.getConstructor(WorkUnitState.class);
                Policy policy = policyConstructor.newInstance(this.taskState, this.metadata, types[i]);
                list.getPolicyList().add((Policy) policy);
            } catch (Exception e) {
                LOG.error(ConfigurationKeys.POLICY_LIST + " contains a class " + policies[i] + " which doesn't extend Policy.");
                throw new Exception(e);
            }
        }
        return list;
    }
    
    public static PolicyCheckerBuilder newBuilder(TaskState state, MetaStoreClient metadata) {
        return new PolicyCheckerBuilder(state, metadata);
    }
    
    public PolicyChecker build() throws Exception {
        return new PolicyChecker(createPolicyList());
    }
}