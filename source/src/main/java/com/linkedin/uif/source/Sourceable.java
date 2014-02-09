package com.linkedin.uif.source;

import java.util.List;
import java.util.Properties;

import com.linkedin.uif.configuration.ExtractorState;
import com.linkedin.uif.configuration.SourceContext;
import com.linkedin.uif.source.extractor.Extractable;
import com.linkedin.uif.source.workunit.Workunit;

public interface Sourceable<S,D,C>
{
  public abstract List<Workunit> getWorkunits(SourceContext context);

  public abstract Extractable<S,D,C> getExtractor(ExtractorState state);
}
