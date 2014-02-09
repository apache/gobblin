package com.linkedin.uif.source.extractor;

import com.linkedin.uif.converter.Convertable;
import com.linkedin.uif.qualitychecker.QualityCheckable;

public interface Extractable<S, D, C>
{

  public D read();

  public S getSchema();

  public void close();

  public Convertable<S, D, C> getConverter();

  public QualityCheckable<C> getQualityChecker();

}
