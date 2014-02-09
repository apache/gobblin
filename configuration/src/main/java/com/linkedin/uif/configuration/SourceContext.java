package com.linkedin.uif.configuration;

import java.util.List;

public class SourceContext extends WritableProperties
{
  private List<ExtractorState> previousStates;
  
  public SourceContext(WritableProperties properties, List<ExtractorState> previousStates)
  {
    addAll(properties);
    this.previousStates = previousStates;
  }
  
  public List<ExtractorState> getPreviousStates()
  {
    return previousStates;
  }

}
