package com.linkedin.uif.converter;

import com.linkedin.uif.source.workunit.WorkUnit;

public interface Converter<SI, SO, DI, DO>
{

  public SO convertSchema(SI inputSchema, WorkUnit workUnit);

  public DO convertRecord(SO outputSchema, DI inputRecord, WorkUnit workUnit);

}
