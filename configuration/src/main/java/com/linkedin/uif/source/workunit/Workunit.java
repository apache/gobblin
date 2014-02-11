package com.linkedin.uif.source.workunit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;

import com.linkedin.uif.configuration.State;

public class WorkUnit extends State
{
  private String namespace = null;
  private String table = null;
  
  public WorkUnit() {}
  
  public WorkUnit(String namespace, String table) {
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
    
    super.readFields(in);
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
    
    super.write(out);
  }
}
