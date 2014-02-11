package com.linkedin.uif.qualitychecker;

public interface QualityChecker<C> {

  public boolean verifyAll();
  
  public boolean verifyRecord(C Record);

}

