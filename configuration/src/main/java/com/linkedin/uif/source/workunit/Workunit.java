package com.linkedin.uif.source.workunit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import com.linkedin.uif.configuration.WritableProperties;

public class Workunit implements Writable
{
  private WritableProperties properties = new WritableProperties();
  private String namespace = null;
  private String table = null;
  
  public Workunit() {}
  
  public Workunit(String namespace, String table) {
    this.namespace = namespace;
    this.table = table;
  }
  
  public String getNamespace()
  {
    return namespace;
  }

  public void setNamespace(String namespace)
  {
    this.namespace = namespace;
  }

  public String getTable()
  {
    return table;
  }

  public void setTable(String table)
  {
    this.table = table;
  }
  
  @Override
  public void readFields(DataInput in) throws IOException
  {
    Text txt = new Text();
    
    txt.readFields(in);
    namespace = txt.toString();
    
    txt.readFields(in);
    table = txt.toString();
    
    properties.readFields(in);
  }

  @Override
  public void write(DataOutput out) throws IOException
  {
    if (namespace == null || table == null)
    {
      throw new RuntimeException("All Workunits must have a namespace and table name");
    }
    Text txt = new Text();
    
    txt.set(namespace);
    txt.write(out);
    
    txt.set(table);
    txt.write(out);
    
    properties.write(out);
  }
}
