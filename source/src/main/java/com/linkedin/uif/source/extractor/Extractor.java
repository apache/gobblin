package com.linkedin.uif.source.extractor;

import com.linkedin.uif.converter.Converter;
import com.linkedin.uif.qualitychecker.QualityCheckable;

public interface Extractor<SI, SO, D, C>
{

  public D read();

  public SI getSchema();

  public void close();

  public Converter<SI, SO, D, C> getConverter();

  public QualityCheckable<C> getQualityChecker();

}
