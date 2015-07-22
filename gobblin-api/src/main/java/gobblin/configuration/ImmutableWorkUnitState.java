/*
 * Copyright (C) 2014-2015 LinkedIn Corp. All rights reserved.
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
import java.io.IOException;
import java.util.Properties;

import gobblin.source.extractor.Watermark;


/**
 * An immutable version of {@link WorkUnitState}.
 *
 * @author ynli
 */
public class ImmutableWorkUnitState extends WorkUnitState {

  public ImmutableWorkUnitState(WorkUnitState workUnitState) {
    super(workUnitState.getWorkunit());
    super.addAll(workUnitState);
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
  public void addAll(State otherState) {
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
  public void addAllIfNotExist(State otherState) {
    throw new UnsupportedOperationException();
  }

  public void overrideWith(Properties properties) {
    throw new UnsupportedOperationException();
  }

  public void overrideWith(State otherState) {
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
