/* (c) 2014 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.configuration;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import com.google.common.base.Splitter;


/**
 * A serializable wrapper class that can be persisted for {@link java.util.Properties}.
 *
 * @author kgoodhop
 */
public class State implements Writable {

  private String id;

  private final Properties properties = new Properties();

  /**
   * Populates this instance with properties of the other instance.
   *
   * @param otherState the other {@link State} instance
   */
  public void addAll(State otherState) {
    this.properties.putAll(otherState.properties);
  }

  /**
   * Populates this instance with values of a {@link Properties} instance.
   *
   * @param properties a {@link Properties} instance
   */
  public void addAll(Properties properties) {
    this.properties.putAll(properties);
  }

  /**
   * Set the id used for state persistence and logging.
   *
   * @param id id of this instance
   */
  public void setId(String id) {
    this.id = id;
  }

  /**
   * Get the id of this instance.
   *
   * @return id of this instance
   */
  public String getId() {
    return this.id;
  }

  /**
   * Set a property.
   *
   * <p>
   *   Both key and value are stored as strings.
   * </p>
   *
   * @param key property key
   * @param value property value
   */
  public void setProp(String key, Object value) {
    properties.put(key, value.toString());
  }

  /**
   * Get the value of a property.
   *
   * @param key property key
   * @return value associated with the key as a string or <code>null</code> if the property is not set
   */
  public String getProp(String key) {
    return getProperty(key);
  }

  /**
   * Get the value of a property, using the given default value if the property is not set.
   *
   * @param key property key
   * @param def default value
   * @return value associated with the key or the default value if the property is not set
   */
  public String getProp(String key, String def) {
    return getProperty(key, def);
  }

  /**
   * Get the value of a property as a {@link java.util.List} of strings.
   *
   * @param key property key
   * @return value associated with the key as a {@link java.util.List} of strings
   */
  public List<String> getPropAsList(String key) {
    return Splitter.on(",").trimResults().omitEmptyStrings().splitToList(getProperty(key));
  }

  /**
   * Get the value of a property as a list of strings, using the given default value if the property is not set.
   *
   * @param key property key
   * @param def default value
   * @return value (the default value if the property is not set) associated with the key as a list of strings
   */
  public List<String> getPropAsList(String key, String def) {
    return Splitter.on(",").trimResults().splitToList(getProperty(key, def));
  }

  /**
   * Get the value of a property as a long integer.
   *
   * @param key property key
   * @return long integer value associated with the key
   */
  public long getPropAsLong(String key) {
    return Long.parseLong(getProperty(key));
  }

  /**
   * Get the value of a property as a long integer, using the given default value if the property is not set.
   *
   * @param key property key
   * @param def default value
   * @return long integer value associated with the key or the default value if the property is not set
   */
  public long getPropAsLong(String key, long def) {
    return Long.parseLong(getProperty(key, String.valueOf(def)));
  }

  /**
   * Get the value of a property as an integer.
   *
   * @param key property key
   * @return integer value associated with the key
   */
  public int getPropAsInt(String key) {
    return Integer.parseInt(getProperty(key));
  }

  /**
   * Get the value of a property as an integer, using the given default value if the property is not set.
   *
   * @param key property key
   * @param def default value
   * @return integer value associated with the key or the default value if the property is not set
   */
  public int getPropAsInt(String key, int def) {
    return Integer.parseInt(getProperty(key, String.valueOf(def)));
  }

  /**
   * Get the value of a property as a double.
   *
   * @param key property key
   * @return double value associated with the key
   */
  public double getPropAsDouble(String key) {
    return Double.parseDouble(getProperty(key));
  }

  /**
   * Get the value of a property as a double, using the given default value if the property is not set.
   *
   * @param key property key
   * @param def default value
   * @return double value associated with the key or the default value if the property is not set
   */
  public double getPropAsDouble(String key, double def) {
    return Double.parseDouble(getProperty(key, String.valueOf(def)));
  }

  /**
   * Get the value of a property as a boolean.
   *
   * @param key property key
   * @return boolean value associated with the key
   */
  public boolean getPropAsBoolean(String key) {
    return Boolean.parseBoolean(getProperty(key));
  }

  /**
   * Get the value of a property as a boolean, using the given default value if the property is not set.
   *
   * @param key property key
   * @param def default value
   * @return boolean value associated with the key or the default value if the property is not set
   */
  public boolean getPropAsBoolean(String key, boolean def) {
    return Boolean.parseBoolean(getProperty(key, String.valueOf(def)));
  }

  protected String getProperty(String key) {
    return properties.getProperty(key);
  }

  protected String getProperty(String key, String def) {
    return properties.getProperty(key, def);
  }

  /**
   * Get the names of all the properties set in a {@link java.util.Set}.
   *
   * @return names of all the properties set in a {@link java.util.Set}
   */
  public Set<String> getPropertyNames() {
    return this.properties.stringPropertyNames();
  }

  /**
   * Check if a property is set.
   *
   * @param key property key
   * @return <code>true</code> if the property is set or <code>false</code> otherwise
   */
  public boolean contains(String key) {
    return properties.getProperty(key) != null;
  }

  @Override
  public void readFields(DataInput in)
      throws IOException {
    Text txt = new Text();

    int numEntries = in.readInt();

    while (numEntries-- > 0) {
      txt.readFields(in);
      String key = txt.toString();
      txt.readFields(in);
      String value = txt.toString();

      properties.put(key, value);
    }
  }

  @Override
  public void write(DataOutput out)
      throws IOException {
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
