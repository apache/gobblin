package com.linkedin.uif.source.workunit;

public class RestWorkunit extends ConnectionBasedWorkunit
{

  public RestWorkunit(String namespace, String table, String url, String username, String password) throws IllegalArgumentException
  {
    super(namespace, table, url, username, password);
  }

}
