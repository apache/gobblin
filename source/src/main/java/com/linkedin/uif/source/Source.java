package com.linkedin.uif.source;

import java.util.List;
import java.util.Properties;

import com.linkedin.uif.configuration.ExtractorState;
import com.linkedin.uif.configuration.SourceContext;
import com.linkedin.uif.source.extractor.Extractor;
import com.linkedin.uif.source.workunit.Workunit;

public interface Source<SI,SO,D,C>
{
  public abstract List<Workunit> getWorkunits(SourceContext context);

  public abstract Extractor<SI,SO,D,C> getExtractor(ExtractorState state);
}
