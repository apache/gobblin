package com.linkedin.uif.source.extractor.resultset;

import java.util.Properties;

import org.apache.avro.Schema;

import com.linkedin.uif.source.workunit.ConnectionBasedWorkunit;

public class SFDCRecordSet<D> extends RestApiRecordSet<D>
{

  // @Override
  // public Iterator<D> iterator()
  // {
  // return new SFDCResultSetIterator();
  // }
  //
  // private static class SFDCResultSetIterator implements Iterator<D>
  // {
  //
  // }

  @Override
  public boolean hasNext()
  {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public void remove()
  {
    // TODO Auto-generated method stub

  }

  @Override
  public Schema getSchema()
  {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void close()
  {
    // TODO Auto-generated method stub

  }

  @Override
  protected ConnectionBasedWorkunit getConnection(Properties extractConf)
  {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  long getExpectedCount()
  {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  long getActualCount()
  {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public D next()
  {
    // TODO Auto-generated method stub
    return null;
  }
}
