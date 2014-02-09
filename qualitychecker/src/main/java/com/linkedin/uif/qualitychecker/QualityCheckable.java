package com.linkedin.uif.qualitychecker;

public interface QualityCheckable<C> {

  public boolean verifyAll();
  
  public boolean verifyRecord(C Record);

}

