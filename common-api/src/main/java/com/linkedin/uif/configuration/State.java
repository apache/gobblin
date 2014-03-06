package com.linkedin.uif.configuration;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import com.google.common.base.Splitter;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class State implements Writable
{

  private String id;

  private Properties properties = new Properties();

  public void addAll(State otherState) {
    this.properties.putAll(otherState.properties);
  }

  public void addAll(Properties properties) {
    this.properties.putAll(properties);
  }

  public void setId(String id)
  {
      this.id = id;
  }

  public String getId()
  {
      return this.id;
  }

  public void setProp(String key, Object value) {
    properties.put(key, value.toString());
  }

  public String getProp(String key) {
    return getProperty(key);
  }

  public String getProp(String key, String def) {
    return getProperty(key, def);
  }

  public List<String> getPropAsList(String key) {
    return Splitter.on(",").trimResults().splitToList(getProperty(key));
  }

  public List<String> getPropAsList(String key, String def) {
    return Splitter.on(",").trimResults().splitToList(getProperty(key, def));
  }

  public long getPropAsLong(String key) {
    return Long.valueOf(getProperty(key));
  }

  public long getPropAsLong(String key, long def) {
    return Long.valueOf(getProperty(key, String.valueOf(def)));
  }

  public int getPropAsInt(String key) {
    return Integer.valueOf(getProperty(key));
  }

  public int getPropAsInt(String key, int def) {
    return Integer.valueOf(getProperty(key, String.valueOf(def)));
  }

  public double getPropAsDouble(String key) {
    return Double.valueOf(getProperty(key));
  }

  public double getPropAsDouble(String key, double def) {
    return Double.valueOf(getProperty(key, String.valueOf(def)));
  }

  public boolean getPropAsBoolean(String key) {
    return Boolean.valueOf(getProperty(key));
  }

  public boolean getPropAsBoolean(String key, boolean def) {
    return Boolean.valueOf(getProperty(key, String.valueOf(def)));
  }
  
  protected String getProperty(String key) {
    return properties.getProperty(key);
  }
  
  protected String getProperty(String key, String def) {
    return properties.getProperty(key, def);
  }

  public Set<String> getPropertyNames() {
    return this.properties.stringPropertyNames();
  }

  public boolean contains(String key) {
    return properties.getProperty(key) != null;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    Text txt = new Text();

    int numEntries = in.readInt();

    while (numEntries-- > 0)
    {
      txt.readFields(in);
      String key = txt.toString();
      txt.readFields(in);
      String value = txt.toString();

      properties.put(key, value);
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    Text txt = new Text();
    out.writeInt(properties.size());

    for (Object key : properties.keySet()) {
      txt.set((String) key);
      txt.write(out);

      txt.set(properties.getProperty((String) key));
      txt.write(out);
    }
  }
}
