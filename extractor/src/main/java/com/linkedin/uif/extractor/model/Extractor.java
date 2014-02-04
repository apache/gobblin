package com.linkedin.uif.extractor.model;

import java.util.Properties;

import com.linkedin.uif.extractor.resultset.RecordSet;

public abstract class Extractor<D, S>
{
  RecordSet<D> rs = null;
  Properties extractConf = null;

  public Extractor(Properties extractConf)
  {
    this.extractConf = extractConf;
  }

  public D read()
  {
    assert extractConf != null;

    // initiate ResultSet for first time
    if (rs == null)
    {
      rs = getRecordSet(extractConf);
    }

    // iterate through all records
    if (rs.hasNext())
      return rs.next();
    else
    {
      rs.close();
      return null;
    }

  };

  public abstract S getSchema();

  protected abstract RecordSet<D> getRecordSet(Properties extractConf);

}
