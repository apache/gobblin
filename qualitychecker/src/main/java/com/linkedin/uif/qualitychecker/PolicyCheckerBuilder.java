package com.linkedin.uif.qualitychecker;

import java.lang.reflect.Constructor;

import com.linkedin.uif.configuration.WorkUnitState;
import com.linkedin.uif.metadata.MetadataCollector;

/**
 * Creates a PolicyChecker and initializes the PolicyList
 * the list is Policies to create is taken from the
 * MetadataCollector
 */
public class PolicyCheckerBuilder
{   
    private MetadataCollector metadata;
    
    public PolicyCheckerBuilder(MetadataCollector metadata) {
        this.metadata = metadata;
    }
    
    @SuppressWarnings("unchecked")
    public PolicyList createPolicyList() throws Exception {
        PolicyList list = new PolicyList();
        String[] policies = this.metadata.getProperties().getProperty("uif.qualitychecker.policies").split(",");
        for (String policyString : policies) {
            try {
                Class<? extends Policy> policyClass = (Class<? extends Policy>) Class.forName(policyString);
                Constructor<? extends Policy> policyConstructor = policyClass.getConstructor(WorkUnitState.class);
                Policy policy = policyConstructor.newInstance(this.metadata);
                list.getPolicyList().add((Policy) policy);
            } catch (Exception e) {
                // log.error("uif.qualitychecker.policies contains a class " + policyString + " which doesn't extend Policy.");
                throw new Exception(e);
            }
        }
        return list;
    }
    
    public static PolicyCheckerBuilder newBuilder(MetadataCollector metadata) {
        return new PolicyCheckerBuilder(metadata);
    }
    
    public PolicyChecker build() throws Exception {
        return new PolicyChecker(createPolicyList());
    }
}