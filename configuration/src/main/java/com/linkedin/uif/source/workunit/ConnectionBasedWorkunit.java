package com.linkedin.uif.source.workunit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;

public abstract class ConnectionBasedWorkunit extends Workunit
{
  private String url;
  private String username;
  private String password;

  public ConnectionBasedWorkunit(String namespace, String table, String url, String username, String password) throws IllegalArgumentException
  {
    super(namespace, table);
    this.url = url;
    this.username = username;
    this.password = password;
  }

  public String getUrl()
  {
    return url;
  }

  public void setUrl(String url)
  {
    this.url = url;
  }

  public String getUsername()
  {
    return username;
  }

  public void setUsername(String username)
  {
    this.username = username;
  }

  public String getPassword()
  {
    return password;
  }

  public void setPassword(String password)
  {
    this.password = password;
  }
  
  @Override
  public void write(DataOutput out) throws IOException
  {
    super.write(out);
    
    if (url == null || username == null || password == null)
    {
      throw new RuntimeException("All connection based workunits must have a url, username, and password");
    }
    
    Text txt = new Text();
    
    txt.set(url);
    txt.write(out);
    
    txt.set(username);
    txt.write(out);
    
    txt.set(password);
    txt.write(out);
  }
  
  @Override
  public void readFields(DataInput in) throws IOException
  {
    super.readFields(in);
    
    Text txt = new Text();
    
    txt.readFields(in);
    url = txt.toString();
    
    txt.readFields(in);
    username = txt.toString();
    
    txt.readFields(in);
    password = txt.toString();
  }
}
