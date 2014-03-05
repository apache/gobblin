package com.linkedin.uif.qualitychecker;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.linkedin.uif.configuration.ConfigurationKeys;
import com.linkedin.uif.configuration.State;

/**
 * Creates a PolicyChecker and initializes the PolicyList
 * the list is Policies to create is taken from the
 * MetadataCollector
 */
public class PolicyCheckerBuilder
{   
    private final State state;
    
    private static final Log LOG = LogFactory.getLog(PolicyCheckerBuilder.class);
    
    public PolicyCheckerBuilder(State state) {
        this.state = state;
    }
    
    @SuppressWarnings("unchecked")
    private List<Policy> createPolicyList() throws Exception {
        List<Policy> list = new ArrayList<Policy>();
        Splitter splitter = Splitter.on(",").omitEmptyStrings().trimResults();
        List<String> policies = Lists.newArrayList(splitter.split(this.state.getProp(ConfigurationKeys.QUALITY_CHECKER_PREFIX + ConfigurationKeys.POLICY_LIST)));
        List<String> types = Lists.newArrayList(splitter.split(this.state.getProp(ConfigurationKeys.QUALITY_CHECKER_PREFIX + ConfigurationKeys.POLICY_LIST_TYPE)));
        if (policies.size() != types.size() ) throw new Exception("Policies list and Policies list type are not the same length");
        for (int i = 0; i < policies.size(); i++) {
            try {
                Class<? extends Policy> policyClass = (Class<? extends Policy>) Class.forName(policies.get(i));
                Constructor<? extends Policy> policyConstructor = policyClass.getConstructor(State.class, Policy.Type.class);
                Policy policy = policyConstructor.newInstance(this.state, Policy.Type.valueOf(types.get(i)));
                list.add((Policy) policy);
            } catch (Exception e) {
                LOG.error(ConfigurationKeys.QUALITY_CHECKER_PREFIX + ConfigurationKeys.POLICY_LIST + " contains a class " + policies.get(i) + " which doesn't extend Policy.", e);
                throw e;
            }
        }
        return list;
    }
    
    public static PolicyCheckerBuilder newBuilder(State state) {
        return new PolicyCheckerBuilder(state);
    }
    
    public PolicyChecker build() throws Exception {
        return new PolicyChecker(createPolicyList());
    }
}
