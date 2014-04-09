package com.linkedin.uif.qualitychecker.row;

import com.linkedin.uif.configuration.ConfigurationKeys;
import com.linkedin.uif.configuration.State;

/**
 * A policy that operates on each row
 * and executes a given check
 * @author stakiar
 */
public abstract class RowLevelPolicy
{    
    private final State state;
    private final Type type;
    
    public enum Type {
        FAIL,          // Fail if the test does not pass
        ERR_FILE,      // Write record to error file
        OPTIONAL       // The test is optional
    };
    
    public enum Result {
      PASSED,          // The test passed
      FAILED           // The test failed
    };
    
    public RowLevelPolicy(State state, RowLevelPolicy.Type type) {
        this.state = state;
        this.type = type;
    }

    public State getTaskState()
    {
        return state;
    }

    public Type getType()
    {
        return type;
    }
    
    public String getErrFileLocation()
    {
        return this.state.getProp(ConfigurationKeys.ROW_LEVEL_ERR_FILE);
    }
    
    public String getFileSystemURI()
    {
        return this.state.getProp(ConfigurationKeys.WRITER_FILE_SYSTEM_URI);
    }
        
    public abstract Result executePolicy(Object record);
    
    @Override
    public String toString() {
        return this.getClass().getName();
    }
 }
