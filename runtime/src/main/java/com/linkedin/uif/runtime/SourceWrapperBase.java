package com.linkedin.uif.runtime;

import java.io.IOException;
import java.util.List;

import com.linkedin.uif.configuration.ConfigurationKeys;
import com.linkedin.uif.configuration.SourceState;
import com.linkedin.uif.configuration.WorkUnitState;
import com.linkedin.uif.source.Source;
import com.linkedin.uif.source.extractor.Extractor;
import com.linkedin.uif.source.workunit.WorkUnit;

public class SourceWrapperBase implements Source {
  private Source<?, ?> source;
  
  public void init(SourceState sourceState) throws InstantiationException, IllegalAccessException, ClassNotFoundException {
    source = (Source<?, ?>) Class.forName(
        sourceState.getProp(ConfigurationKeys.SOURCE_CLASS_KEY))
        .newInstance();
  }

  @Override
  public List<WorkUnit> getWorkunits(SourceState state) {
    return source.getWorkunits(state);
  }

  @Override
  public Extractor getExtractor(WorkUnitState state) throws IOException {
    return source.getExtractor(state);
  }

  @Override
  public void shutdown(SourceState state) {
    source.shutdown(state);    
  }

}
