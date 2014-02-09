package com.linkedin.uif.converter;

public interface Convertable<S,D,C> {
  
  public S convertSchema(S schema);

  public C convertRecord(S schema, D inputRecord);
  
}

