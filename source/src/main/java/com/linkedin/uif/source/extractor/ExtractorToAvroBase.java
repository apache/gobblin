package com.linkedin.uif.source.extractor;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import com.linkedin.uif.configuration.ExtractorState;
import com.linkedin.uif.converter.Converter;
import com.linkedin.uif.qualitychecker.QualityChecker;

public abstract class ExtractorToAvroBase<SI,D> implements Extractor<SI, Schema,D,GenericRecord>
{
  private ExtractorState state;
  private QualityChecker qc;

  public ExtractorToAvroBase(ExtractorState state)
  {
    this.state = state;
    this.qc = getQualityChecker();
  }
  
  public abstract SI getSchema();
  
  public abstract Converter<SI, Schema, D, GenericRecord> getConverter();
  
  public abstract D read();
  
  public abstract void close();

  protected void incrementPulledCount(long count)
  {
    qc.incrementPulledCount(count);
  }
  
  @Override
  public QualityChecker getQualityChecker()
  {
    return new QualityChecker();
  }

}
