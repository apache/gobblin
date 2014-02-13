package com.linkedin.uif.qualitychecker;

import java.util.ArrayList;

/**
 * Wrapper around an ArrayList of Policies
 */
public class PolicyList
{
    private ArrayList<Policy> policyList;
    
    public PolicyList() {
        this.policyList = new ArrayList<Policy>();
    }
    
    public ArrayList<Policy> getPolicyList() {
        return this.policyList;
    }
}
