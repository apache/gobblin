package com.linkedin.uif.source.extractor.resultset;

import java.util.Iterator;
import java.util.Properties;

import org.apache.avro.Schema;

import com.linkedin.uif.source.workunit.ConnectionBasedWorkunit;

public abstract class FileRecordSet<String> implements Iterator<String>
{

  abstract long getExpectedCount();

  abstract long getActualCount();

  public abstract Schema getSchema();

  public abstract void close();

  @Override
  public abstract String next();

  protected abstract ConnectionBasedWorkunit getConnection(Properties extractConf);

}
