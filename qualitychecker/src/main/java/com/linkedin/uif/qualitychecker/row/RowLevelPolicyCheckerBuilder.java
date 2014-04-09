package com.linkedin.uif.qualitychecker.row;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.linkedin.uif.configuration.ConfigurationKeys;
import com.linkedin.uif.configuration.State;

public class RowLevelPolicyCheckerBuilder
{
    private final State state;
    
    private static final Logger LOG = LoggerFactory.getLogger(RowLevelPolicyCheckerBuilder.class);
    
    public RowLevelPolicyCheckerBuilder(State state) {
        this.state = state;
    }
    
    @SuppressWarnings("unchecked")
    private List<RowLevelPolicy> createPolicyList() throws Exception {
        List<RowLevelPolicy> list = new ArrayList<RowLevelPolicy>();
        Splitter splitter = Splitter.on(",").omitEmptyStrings().trimResults();
        List<String> policies = Lists.newArrayList(splitter.split(this.state.getProp(ConfigurationKeys.ROW_LEVEL_POLICY_LIST)));
        List<String> types = Lists.newArrayList(splitter.split(this.state.getProp(ConfigurationKeys.ROW_LEVEL_POLICY_LIST_TYPE)));
        if (policies.size() != types.size() ) throw new Exception("Row Policies list and Row Policies list type are not the same length");
        for (int i = 0; i < policies.size(); i++) {
            try {
                Class<? extends RowLevelPolicy> policyClass = (Class<? extends RowLevelPolicy>) Class.forName(policies.get(i));
                Constructor<? extends RowLevelPolicy> policyConstructor = policyClass.getConstructor(State.class, RowLevelPolicy.Type.class);
                RowLevelPolicy policy = policyConstructor.newInstance(this.state, RowLevelPolicy.Type.valueOf(types.get(i)));
                list.add(policy);
            } catch (Exception e) {
                LOG.error(ConfigurationKeys.ROW_LEVEL_POLICY_LIST + " contains a class " + policies.get(i) + " which doesn't extend RowLevelPolicy.", e);
                throw e;
            }
        }
        return list;
    }
    
    public static RowLevelPolicyCheckerBuilder newBuilder(State state) {
        return new RowLevelPolicyCheckerBuilder(state);
    }
    
    public RowLevelPolicyChecker build() throws Exception {
        return new RowLevelPolicyChecker(createPolicyList());
    }
}
