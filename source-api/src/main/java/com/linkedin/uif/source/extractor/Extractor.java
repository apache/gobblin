package com.linkedin.uif.source.extractor;

import com.linkedin.uif.configuration.TaskState;

public abstract class Extractor<S, D>
{
  private TaskState state;

  public Extractor(TaskState state)
  {
    this.state = state;
  }

  public abstract S getSchema();

  public abstract D readRecord();

  public abstract void close();

  public abstract long getPulledRecordCount();
}
