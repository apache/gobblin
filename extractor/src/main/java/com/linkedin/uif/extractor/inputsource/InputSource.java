package com.linkedin.uif.extractor.inputsource;

import java.util.Properties;

import com.linkedin.uif.extractor.model.Extractor;

public abstract class InputSource
{
  public abstract WorkUnit getWorkunit(Properties extractConf);

  public abstract Extractor getExtractor(Properties extractConf);
}
