package com.linkedin.uif.qualitychecker;

import org.apache.avro.generic.GenericRecord;

public class QualityChecker implements QualityCheckable<GenericRecord>
{
  private long pulledCount = 0;
  private long writtenCount = 0;

  @Override
  public boolean verifyAll()
  {
    return pulledCount == writtenCount;
  }

  @Override
  public boolean verifyRecord(GenericRecord Record)
  {
    return true;
  }
  
  public void incrementPulledCount(long count)
  {
    pulledCount += count;
  }
  
  public void incrementWrittenCount(long count)
  {
    writtenCount += count;
  }

}
