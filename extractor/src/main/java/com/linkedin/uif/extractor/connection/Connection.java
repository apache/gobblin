package com.linkedin.uif.extractor.connection;

import java.util.Properties;

public abstract class Connection
{
  Properties extractConf;

  public Connection(Properties extractConf) throws IllegalArgumentException
  {

    if (extractConf.get("url") == null)
      throw new IllegalArgumentException("url cannot be null");
    if (extractConf.get("username") == null)
      throw new IllegalArgumentException("url cannot be null");
    if (extractConf.get("password") == null)
      throw new IllegalArgumentException("url cannot be null");

    this.extractConf = extractConf;

  }
}
