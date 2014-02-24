package com.linkedin.uif.qualitychecker;

import java.util.ArrayList;
import java.util.List;

/**
 * Wrapper around an ArrayList of Policies
 */
public class PolicyList
{
    private List<Policy> policyList;
    
    public PolicyList() {
        this.policyList = new ArrayList<Policy>();
    }
    
    public List<Policy> getPolicyList() {
        return this.policyList;
    }
}
