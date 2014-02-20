package com.linkedin.uif.configuration;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Properties;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;


public class State implements Writable {
  private Properties properties = new Properties();

  public void addAll(State otherState) {
    this.properties.putAll(otherState.properties);
  }

  public void addAll(Properties properties) {
    properties.putAll(properties);
  }

  public void setProp(String key, Object value) {
    properties.put(key, value.toString());
  }

  public String getProp(String key) {
    return properties.getProperty(key);
  }

  public String getProp(String key, String def) {
    return properties.getProperty(key, def);
  }

  public String[] getPropAsList(String key) {
    if (!contains(key))
      return new String[0];

    return properties.getProperty(key).split(",");
  }

  public String[] getPropAsList(String key, String def) {
    return properties.getProperty(key, def).split(",");
  }

  public long getPropAsLong(String key) {
    return Long.valueOf(properties.getProperty(key));
  }

  public long getPropAsLong(String key, long def) {
    return Long.valueOf(properties.getProperty(key, String.valueOf(def)));
  }

  public int getPropAsInt(String key) {
    return Integer.valueOf(properties.getProperty(key));
  }

  public int getPropAsInt(String key, int def) {
    return Integer.valueOf(properties.getProperty(key, String.valueOf(def)));
  }

  public double getPropAsDouble(String key) {
    return Double.valueOf(properties.getProperty(key));
  }

  public double getPropAsDouble(String key, double def) {
    return Double.valueOf(properties.getProperty(key, String.valueOf(def)));
  }

  public boolean getPropAsBoolean(String key) {
    return Boolean.valueOf(properties.getProperty(key));
  }

  public boolean getPropAsBoolean(String key, boolean def) {
    return Boolean.valueOf(properties.getProperty(key, String.valueOf(def)));
  }

  public Set<String> getPropertyNames() {
    return this.properties.stringPropertyNames();
  }

  public boolean contains(String key) {
    return properties.contains(key);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    Text txt = new Text();

    int numEntries = in.readInt();

    while (numEntries-- < 0) {
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
