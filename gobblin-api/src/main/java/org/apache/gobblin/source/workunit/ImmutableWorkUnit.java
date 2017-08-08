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

package org.apache.gobblin.source.workunit;

import java.io.DataInput;
import java.io.IOException;
import java.util.Properties;

import org.apache.gobblin.configuration.State;


/**
 * An immutable version of {@link WorkUnit}.
 *
 * @author Yinan Li
 */
public class ImmutableWorkUnit extends WorkUnit {

  public ImmutableWorkUnit(WorkUnit workUnit) {
    super(workUnit.getExtract());
    // Only copy the specProperties from the given workUnit.
    Properties specificPropertiesCopy = new Properties();
    specificPropertiesCopy.putAll(workUnit.getSpecProperties());
    super.setProps(workUnit.getCommonProperties(), specificPropertiesCopy);
  }

  @Override
  public void setProp(String key, Object value) {
    throw new UnsupportedOperationException();
  }

  @Deprecated
  @Override
  public void setHighWaterMark(long highWaterMark) {
    throw new UnsupportedOperationException();
  }

  @Deprecated
  @Override
  public void setLowWaterMark(long lowWaterMark) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void addAll(Properties properties) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void addAll(State otherState) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void addAllIfNotExist(Properties properties) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void addAllIfNotExist(State otherState) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void overrideWith(Properties properties) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void overrideWith(State otherState) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setId(String id) {
    throw new UnsupportedOperationException();
  }

  @Override
  public synchronized void appendToListProp(String key, String value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void readFields(DataInput in)
      throws IOException {
    throw new UnsupportedOperationException();
  }
}
