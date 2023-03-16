/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.gobblin.configuration;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

import lombok.EqualsAndHashCode;
import lombok.Getter;

import org.apache.gobblin.compat.hadoop.TextSerializer;
import org.apache.gobblin.compat.hadoop.WritableShim;


/**
 * A serializable wrapper class that can be persisted for {@link Properties}.
 *
 * @author kgoodhop
 */
@EqualsAndHashCode(exclude = {"jsonParser"})
public class State implements WritableShim {

  private static final Joiner LIST_JOINER = Joiner.on(",");
  private static final Splitter LIST_SPLITTER = Splitter.on(",").trimResults().omitEmptyStrings();

  private String id;

  // State contains two parts: commonProperties and specProperties (specProperties overrides commonProperties).
  @Getter
  private Properties commonProperties;
  @Getter
  private Properties specProperties;

  private final JsonParser jsonParser = new JsonParser();

  public State() {
    this.specProperties = new Properties();
    this.commonProperties = new Properties();
  }

  public State(Properties properties) {
    this.specProperties = properties;
    this.commonProperties = new Properties();
  }

  public State(State otherState) {
    this.commonProperties = otherState.getCommonProperties();
    this.specProperties = new Properties();
    this.specProperties.putAll(otherState.getProperties());
    for (Object key : this.commonProperties.keySet()) {
      if (this.specProperties.containsKey(key) && this.commonProperties.get(key).equals(this.specProperties.get(key))) {
        this.specProperties.remove(key);
      }
    }
  }

  /**
   * Return a copy of the underlying {@link Properties} object.
   *
   * @return A copy of the underlying {@link Properties} object.
   */
  public Properties getProperties() {
    // a.putAll(b) iterates over the entries of b. Synchronizing on b prevents concurrent modification on b.
    synchronized (this.specProperties) {
      Properties props = new Properties();
      if (this.commonProperties != null) {
        props.putAll(this.commonProperties);
      }
      props.putAll(this.specProperties);
      return props;
    }
  }

  /**
   * Populates this instance with properties of the other instance.
   *
   * @param otherState the other {@link State} instance
   */
  public void addAll(State otherState) {
    Properties diffCommonProps = new Properties();
    diffCommonProps.putAll(Maps.difference(this.commonProperties, otherState.commonProperties).entriesOnlyOnRight());
    addAll(diffCommonProps);
    addAll(otherState.specProperties);
  }

  /**
   * Populates this instance with values of a {@link Properties} instance.
   *
   * @param properties a {@link Properties} instance
   */
  public void addAll(Properties properties) {
    this.specProperties.putAll(properties);
  }

  /**
   * Add properties in a {@link State} instance that are not in the current instance.
   *
   * @param otherState a {@link State} instance
   */
  public void addAllIfNotExist(State otherState) {
    addAllIfNotExist(otherState.commonProperties);
    addAllIfNotExist(otherState.specProperties);
  }

  /**
   * Add properties in a {@link Properties} instance that are not in the current instance.
   *
   * @param properties a {@link Properties} instance
   */
  public void addAllIfNotExist(Properties properties) {
    for (String key : properties.stringPropertyNames()) {
      if (!this.specProperties.containsKey(key) && !this.commonProperties.containsKey(key)) {
        this.specProperties.setProperty(key, properties.getProperty(key));
      }
    }
  }

  /**
   * Add properties in a {@link State} instance that are in the current instance.
   *
   * @param otherState a {@link State} instance
   */
  public void overrideWith(State otherState) {
    overrideWith(otherState.commonProperties);
    overrideWith(otherState.specProperties);
  }

  /**
   * Add properties in a {@link Properties} instance that are in the current instance.
   *
   * @param properties a {@link Properties} instance
   */
  public void overrideWith(Properties properties) {
    for (String key : properties.stringPropertyNames()) {
      if (this.specProperties.containsKey(key) || this.commonProperties.containsKey(key)) {
        this.specProperties.setProperty(key, properties.getProperty(key));
      }
    }
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
    this.specProperties.put(key, value.toString());
  }

  /**
   * Override existing {@link #commonProperties} and {@link #specProperties}.
   * @param commonProperties
   * @param specProperties
   */
  public void setProps(Properties commonProperties, Properties specProperties) {
    this.commonProperties = commonProperties;
    this.specProperties = specProperties;
  }

  /**
   * Appends the input value to a list property that can be retrieved with {@link #getPropAsList}.
   *
   * <p>
   *   List properties are internally stored as comma separated strings. Adding a value that contains commas (for
   *   example "a,b,c") will essentially add multiple values to the property ("a", "b", and "c"). This is
   *   similar to the way that {@link org.apache.hadoop.conf.Configuration} works.
   * </p>
   *
   * @param key property key
   * @param value property value (if it includes commas, it will be split by the commas).
   */
  public synchronized void appendToListProp(String key, String value) {
    if (contains(key)) {
      setProp(key, LIST_JOINER.join(getProp(key), value));
    } else {
      setProp(key, value);
    }
  }

  /**
   * Appends the input value to a set property that can be retrieved with {@link #getPropAsSet}.
   *
   * <p>
   *   Set properties are internally stored as comma separated strings. Adding a value that contains commas (for
   *   example "a,b,c") will essentially add multiple values to the property ("a", "b", and "c"). This is
   *   similar to the way that {@link org.apache.hadoop.conf.Configuration} works.
   * </p>
   *
   * @param key property key
   * @param value property value (if it includes commas, it will be split by the commas).
   */
  public synchronized void appendToSetProp(String key, String value) {
    Set<String> set = value == null ? Sets.<String>newHashSet() : Sets.newHashSet(LIST_SPLITTER.splitToList(value));
    if (contains(key)) {
      set.addAll(getPropAsSet(key));
    }
    setProp(key, LIST_JOINER.join(set));
  }

  /**
   * Get the value of a property.
   *
   * @param key property key
   * @return value associated with the key as a string or <code>null</code> if the property is not set
   */
  public String getProp(String key) {
    if (this.specProperties.containsKey(key)) {
      return this.specProperties.getProperty(key);
    }
    return this.commonProperties.getProperty(key);
  }

  /**
   * Get the value of a property, using the given default value if the property is not set.
   *
   * @param key property key
   * @param def default value
   * @return value associated with the key or the default value if the property is not set
   */
  public String getProp(String key, String def) {
    if (this.specProperties.containsKey(key)) {
      return this.specProperties.getProperty(key);
    }
    return this.commonProperties.getProperty(key, def);
  }

  /**
   * Get the value of a comma separated property as a {@link List} of strings.
   *
   * @param key property key
   * @return value associated with the key as a {@link List} of strings
   */
  public List<String> getPropAsList(String key) {
    return LIST_SPLITTER.splitToList(getProp(key));
  }

  /**
   * Get the value of a property as a list of strings, using the given default value if the property is not set.
   *
   * @param key property key
   * @param def default value
   * @return value (the default value if the property is not set) associated with the key as a list of strings
   */
  public List<String> getPropAsList(String key, String def) {
    return LIST_SPLITTER.splitToList(getProp(key, def));
  }

  /**
   * Get the value of a comma separated property as a {@link Set} of strings.
   *
   * @param key property key
   * @return value associated with the key as a {@link Set} of strings
   */
  public Set<String> getPropAsSet(String key) {
    return ImmutableSet.copyOf(LIST_SPLITTER.splitToList(getProp(key)));
  }

  /**
   * Get the value of a comma separated property as a {@link Set} of strings.
   *
   * @param key property key
   * @param def default value
   * @return value (the default value if the property is not set) associated with the key as a {@link Set} of strings
   */
  public Set<String> getPropAsSet(String key, String def) {
    return ImmutableSet.copyOf(LIST_SPLITTER.splitToList(getProp(key, def)));
  }

  /**
   * Get the value of a property as a case insensitive {@link Set} of strings.
   *
   * @param key property key
   * @return value associated with the key as a case insensitive {@link Set} of strings
   */
  public Set<String> getPropAsCaseInsensitiveSet(String key) {
    return ImmutableSortedSet.copyOf(String.CASE_INSENSITIVE_ORDER, LIST_SPLITTER.split(getProp(key)));
  }

  /**
   * Get the value of a property as a case insensitive {@link Set} of strings, using the given default value if the property is not set.
   *
   * @param key property key
   * @param def default value
   * @return value associated with the key as a case insensitive {@link Set} of strings
   */
  public Set<String> getPropAsCaseInsensitiveSet(String key, String def) {
    return ImmutableSortedSet.copyOf(String.CASE_INSENSITIVE_ORDER, LIST_SPLITTER.split(getProp(key, def)));
  }

  /**
   * Get the value of a property as a long integer.
   *
   * @param key property key
   * @return long integer value associated with the key
   */
  public long getPropAsLong(String key) {
    return Long.parseLong(getProp(key));
  }

  /**
   * Get the value of a property as a long integer, using the given default value if the property is not set.
   *
   * @param key property key
   * @param def default value
   * @return long integer value associated with the key or the default value if the property is not set
   */
  public long getPropAsLong(String key, long def) {
    return Long.parseLong(getProp(key, String.valueOf(def)));
  }

  /**
   * Get the value of a property as an integer.
   *
   * @param key property key
   * @return integer value associated with the key
   */
  public int getPropAsInt(String key) {
    return Integer.parseInt(getProp(key));
  }

  /**
   * Get the value of a property as an integer, using the given default value if the property is not set.
   *
   * @param key property key
   * @param def default value
   * @return integer value associated with the key or the default value if the property is not set
   */
  public int getPropAsInt(String key, int def) {
    return Integer.parseInt(getProp(key, String.valueOf(def)));
  }

  /**
   * Get the value of a property as a short.
   *
   * @param key property key
   * @return short value associated with the key
   */
  public short getPropAsShort(String key) {
    return Short.parseShort(getProp(key));
  }

  /**
   * Get the value of a property as a short.
   *
   * @param key property key
   * @param radix radix used to parse the value
   * @return short value associated with the key
   */
  public short getPropAsShortWithRadix(String key, int radix) {
    return Short.parseShort(getProp(key), radix);
  }

  /**
   * Get the value of a property as an short, using the given default value if the property is not set.
   *
   * @param key property key
   * @param def default value
   * @return short value associated with the key or the default value if the property is not set
   */
  public short getPropAsShort(String key, short def) {
    return Short.parseShort(getProp(key, String.valueOf(def)));
  }

  /**
   * Get the value of a property as an short, using the given default value if the property is not set.
   *
   * @param key property key
   * @param def default value
   * @param radix radix used to parse the value
   * @return short value associated with the key or the default value if the property is not set
   */
  public short getPropAsShortWithRadix(String key, short def, int radix) {
    return contains(key) ? Short.parseShort(getProp(key), radix) : def;
  }

  /**
   * Get the value of a property as a double.
   *
   * @param key property key
   * @return double value associated with the key
   */
  public double getPropAsDouble(String key) {
    return Double.parseDouble(getProp(key));
  }

  /**
   * Get the value of a property as a double, using the given default value if the property is not set.
   *
   * @param key property key
   * @param def default value
   * @return double value associated with the key or the default value if the property is not set
   */
  public double getPropAsDouble(String key, double def) {
    return Double.parseDouble(getProp(key, String.valueOf(def)));
  }

  /**
   * Get the value of a property as a boolean.
   *
   * @param key property key
   * @return boolean value associated with the key
   */
  public boolean getPropAsBoolean(String key) {
    return Boolean.parseBoolean(getProp(key));
  }

  /**
   * Get the value of a property as a boolean, using the given default value if the property is not set.
   *
   * @param key property key
   * @param def default value
   * @return boolean value associated with the key or the default value if the property is not set
   */
  public boolean getPropAsBoolean(String key, boolean def) {
    return Boolean.parseBoolean(getProp(key, String.valueOf(def)));
  }

  /**
   * Get the value of a property as a {@link JsonArray}.
   *
   * @param key property key
   * @return {@link JsonArray} value associated with the key
   */
  public JsonArray getPropAsJsonArray(String key) {
    JsonElement jsonElement = this.jsonParser.parse(getProp(key));
    Preconditions.checkArgument(jsonElement.isJsonArray(),
        "Value for key " + key + " is malformed, it must be a JsonArray: " + jsonElement);
    return jsonElement.getAsJsonArray();
  }

  /**
   * Remove a property if it exists.
   *
   * @param key property key
   */
  public void removeProp(String key) {
    this.specProperties.remove(key);
    if (this.commonProperties.containsKey(key)) {
      // This case should not happen.
      Properties commonPropsCopy = new Properties();
      commonPropsCopy.putAll(this.commonProperties);
      commonPropsCopy.remove(key);
      this.commonProperties = commonPropsCopy;
    }
  }

  /**
   * Remove all properties with a certain keyPrefix
   *
   * @param prefix key prefix
   */
  public void removePropsWithPrefix(String prefix) {
    this.specProperties.entrySet().removeIf(entry -> ((String) entry.getKey()).startsWith(prefix));

    Properties newCommonProperties = null;
    for (Object key: this.commonProperties.keySet()) {
      if (((String)key).startsWith(prefix)) {
        if (newCommonProperties == null) {
          newCommonProperties = new Properties();
          newCommonProperties.putAll(this.commonProperties);
        }
        newCommonProperties.remove(key);
      }
    }

    if (newCommonProperties != null) {
      this.commonProperties = newCommonProperties;
    }
  }

  /**
   * @deprecated Use {@link #getProp(String)}
   */
  @Deprecated
  protected String getProperty(String key) {
    return getProp(key);
  }

  /**
   * @deprecated Use {@link #getProp(String, String)}
   */
  @Deprecated
  protected String getProperty(String key, String def) {
    return getProp(key, def);
  }

  /**
   * Get the names of all the properties set in a {@link Set}.
   *
   * @return names of all the properties set in a {@link Set}
   */
  public Set<String> getPropertyNames() {
    return Sets.newHashSet(
        Iterables.concat(this.commonProperties.stringPropertyNames(), this.specProperties.stringPropertyNames()));
  }

  /**
   * Check if a property is set.
   *
   * @param key property key
   * @return <code>true</code> if the property is set or <code>false</code> otherwise
   */
  public boolean contains(String key) {
    return this.specProperties.containsKey(key) || this.commonProperties.containsKey(key);
  }

  @Override
  public void readFields(DataInput in)
      throws IOException {
    int numEntries = in.readInt();
    while (numEntries-- > 0) {
      String key = TextSerializer.readTextAsString(in).intern();
      String value = TextSerializer.readTextAsString(in).intern();
      this.specProperties.put(key, value);
    }
  }

  @Override
  public void write(DataOutput out)
      throws IOException {
    out.writeInt(this.commonProperties.size() + this.specProperties.size());
    for (Object key : this.commonProperties.keySet()) {
      TextSerializer.writeStringAsText(out, (String) key);
      TextSerializer.writeStringAsText(out, this.commonProperties.getProperty((String) key));
    }
    for (Object key : this.specProperties.keySet()) {
      TextSerializer.writeStringAsText(out, (String) key);
      TextSerializer.writeStringAsText(out, this.specProperties.getProperty((String) key));
    }
  }

  @Override
  public String toString() {
    return "Common:" + this.commonProperties.toString() + "\n Specific: " + this.specProperties.toString();
  }
}
