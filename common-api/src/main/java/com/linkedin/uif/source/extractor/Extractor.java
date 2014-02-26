package com.linkedin.uif.source.extractor;

import com.linkedin.uif.configuration.WorkUnitState;

public abstract class Extractor<S, D>
{
  private WorkUnitState state;

  public Extractor(WorkUnitState state)
  {
    this.state = state;
  }

  public abstract S getSchema();

  public abstract D readRecord();

  public abstract void close();

  public abstract long getExpectedRecordCount();

  protected WorkUnitState getState() {
    return state;
  }
}
