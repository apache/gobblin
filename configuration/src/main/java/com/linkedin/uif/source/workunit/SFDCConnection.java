package com.linkedin.uif.source.workunit;

public class SFDCConnection extends RestWorkunit
{

  public SFDCConnection(String namespace, String table, String url, String username, String password) throws IllegalArgumentException
  {
    super(namespace, table, url, username, password);
  }

}
