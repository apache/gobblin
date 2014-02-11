package com.linkedin.uif.converter;

import com.linkedin.uif.configuration.SourceState;

public interface Converter<SI,SO,DI,DO> {
  
  public SO convertSchema(SI inputSchema, SourceState state);

  public DO convertRecord(SO outputSchema, DI inputRecord, SourceState state);
  
}

