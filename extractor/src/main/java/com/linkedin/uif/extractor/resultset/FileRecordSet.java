package com.linkedin.uif.extractor.resultset;

import java.util.Iterator;
import java.util.Properties;

import org.apache.avro.Schema;

import com.linkedin.uif.extractor.connection.Connection;

public abstract class FileRecordSet<String> implements Iterator<String>
{

  abstract long getExpectedCount();

  abstract long getActualCount();

  public abstract Schema getSchema();

  public abstract void close();

  @Override
  public abstract String next();

  protected abstract Connection getConnection(Properties extractConf);

}
