package com.linkedin.uif.converter;

import com.linkedin.uif.configuration.SourceContext;

public interface Converter<SI,SO,D,C> {
  
  public SO convertSchema(SI inputSchema, SourceContext context);

  public C convertRecord(SO outputSchema, D inputRecord, SourceContext context);
  
}

