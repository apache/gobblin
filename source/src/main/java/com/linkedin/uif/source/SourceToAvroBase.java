package com.linkedin.uif.source;

import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import com.linkedin.uif.configuration.ExtractorState;
import com.linkedin.uif.configuration.SourceContext;
import com.linkedin.uif.source.extractor.Extractor;
import com.linkedin.uif.source.workunit.Workunit;

public abstract class SourceToAvroBase<SI,D> implements Source<SI,Schema, D, GenericRecord>
{
  
  public abstract List<Workunit> getWorkunits(SourceContext context);

  public abstract Extractor<SI, Schema, D, GenericRecord> getExtractor(ExtractorState state);

}
