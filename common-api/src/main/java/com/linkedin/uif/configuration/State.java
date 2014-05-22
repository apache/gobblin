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

/**
 * <p>
 * Serializable property class for persisting state among different runtime instances
 * </p>
 * 
 * @author kgoodhop
 *
 */
public class State implements Writable
{
  private String id;

  private Properties properties = new Properties();

  /**
   * Populates this instance with the properties of the other instance.
   * 
   * @param otherState
   */
  public void addAll(State otherState) {
    this.properties.putAll(otherState.properties);
  }

  /**
   * Populates this instance with the values of {@link Properties}
   * @param properties
   */
  public void addAll(Properties properties) {
    this.properties.putAll(properties);
  }

  /**
   * Id used for state persistence and logging.
   * @param id
   */
  public void setId(String id)
  {
      this.id = id;
  }

  /**
   * 
   * @return this instance's id
   */
  public String getId()
  {
      return this.id;
  }

  /**
   * Setter for property that will be persisted, value will be stored as string.
   * @param key
   * @param value
   */
  public void setProp(String key, Object value) {
    properties.put(key, value.toString());
  }

  /**
   * Getter
   * @param key 
   * @return String value associated with key
   */
  public String getProp(String key) {
    return getProperty(key);
  }

  /**
   * Getter with default
   * @param key
   * @param def value returned if key is not set
   * @return
   */
  public String getProp(String key, String def) {
    return getProperty(key, def);
  }

  /**
   * Converts comma delimited value into List<String> 
   * @param key
   * @return List<String> value associated with key
   */
  public List<String> getPropAsList(String key) {
    return Splitter.on(",").trimResults().splitToList(getProperty(key));
  }

  /**
   * Converts comma delimited value into List<String> 
   * @param key
   * @param def value parsed into List if key is not set
   * @return List<String> value associated with key
   */
  public List<String> getPropAsList(String key, String def) {
    return Splitter.on(",").trimResults().splitToList(getProperty(key, def));
  }

  /**
   * Parses value as long
   * @param key
   * @return long value associated with key
   */
  public long getPropAsLong(String key) {
    return Long.valueOf(getProperty(key));
  }

  /**
   * Parses value as long
   * @param key
   * @param def
   * @return long value associated with key or def if key is not set
   */
  public long getPropAsLong(String key, long def) {
    return Long.valueOf(getProperty(key, String.valueOf(def)));
  }

  /**
   * Parses value as int 
   * @param key
   * @return int value associated with key
   */
  public int getPropAsInt(String key) {
    return Integer.valueOf(getProperty(key));
  }

  /**
   * Parses value as int 
   * @param key
   * @param def
   * @return int value associated with key or def if key is not set
   */
  public int getPropAsInt(String key, int def) {
    return Integer.valueOf(getProperty(key, String.valueOf(def)));
  }

  /**
   * Parses value as double
   * @param key
   * @return double value associated with key
   */
  public double getPropAsDouble(String key) {
    return Double.valueOf(getProperty(key));
  }

  /**
   * Parses value as double
   * @param key
   * @param def
   * @return double value associated with key or def if key is not set
   */
  public double getPropAsDouble(String key, double def) {
    return Double.valueOf(getProperty(key, String.valueOf(def)));
  }

  /**
   * Parses value as boolean
   * @param key
   * @return boolean value associated with key
   */
  public boolean getPropAsBoolean(String key) {
    return Boolean.valueOf(getProperty(key));
  }

  /**
   * Parses value as boolean
   * @param key
   * @param def
   * @return boolean value associated with key or def if key is not set
   */
  public boolean getPropAsBoolean(String key, boolean def) {
    return Boolean.valueOf(getProperty(key, String.valueOf(def)));
  }
  
  protected String getProperty(String key) {
    return properties.getProperty(key);
  }
  
  protected String getProperty(String key, String def) {
    return properties.getProperty(key, def);
  }

  /**
   * Returns all the properties currently set in this {@link State}
   * @return
   */
  public Set<String> getPropertyNames() {
    return this.properties.stringPropertyNames();
  }

  /**
   * Returns true if this {@link State} has this key set
   * @param key
   * @return
   */
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
  
  @Override
  public String toString() {
    return properties.toString();
  }
}
