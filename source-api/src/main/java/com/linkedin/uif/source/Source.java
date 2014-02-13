package com.linkedin.uif.source;

import java.util.List;

import com.linkedin.uif.configuration.SourceState;
import com.linkedin.uif.configuration.WorkUnitState;
import com.linkedin.uif.source.extractor.Extractor;
import com.linkedin.uif.source.workunit.WorkUnit;

public interface Source<S, D>
{
  public abstract List<WorkUnit> getWorkunits(SourceState state);

  public abstract Extractor<S, D> getExtractor(WorkUnitState state);
  
  public abstract void publishSourceMeta(SourceState state);
}
