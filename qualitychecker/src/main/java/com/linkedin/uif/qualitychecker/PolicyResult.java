package com.linkedin.uif.qualitychecker;

/**
 * Output of a Policy's execute method
 */
public class PolicyResult
{
    private Status status;
    
    /**
     * PASSED: Test passed with no problems
     * FAILED: Test failed and the job should fail also
     * OPTIONAL: Test failed but the job should not fail
     * RETRY: Test failed and the job should force a retry
     */
    public enum Status { PASSED, FAILED, OPTIONAL, RETRY };
    
    public PolicyResult(Status status) {
        this.status = status;
    }
    
    public Status getStatus() {
        return this.status;
    }
    
    public void setStatus(Status status) {
        this.status = status;
    }
    
    @Override
    public String toString() {
        return this.status.toString();
    }
}
