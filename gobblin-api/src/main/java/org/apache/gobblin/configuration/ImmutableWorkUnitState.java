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
import java.io.IOException;
import java.util.Properties;

import org.apache.gobblin.source.extractor.Watermark;


/**
 * An immutable version of {@link WorkUnitState}.
 *
 * @author Yinan Li
 */
public class ImmutableWorkUnitState extends WorkUnitState {

  public ImmutableWorkUnitState(WorkUnitState workUnitState) {
    super(workUnitState.getWorkunit(), workUnitState.getJobState());
    super.addAll(workUnitState.getSpecProperties());
  }

  @Override
  public void setWorkingState(WorkingState state) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setActualHighWatermark(Watermark watermark) {
    throw new UnsupportedOperationException();
  }

  @Deprecated
  @Override
  public void setHighWaterMark(long value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setProp(String key, Object value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void addAll(Properties properties) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void addAllIfNotExist(Properties properties) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void overrideWith(Properties properties) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setId(String id) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void backoffActualHighWatermark() {
    throw new UnsupportedOperationException();
  }

  @Override
  public synchronized void appendToListProp(String key, String value) {
    throw new UnsupportedOperationException();
  }
}
