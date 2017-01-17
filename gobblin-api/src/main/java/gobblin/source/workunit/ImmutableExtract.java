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

package gobblin.source.workunit;

import java.io.DataInput;
import java.io.IOException;
import java.util.Properties;

import gobblin.configuration.SourceState;
import gobblin.configuration.State;


/**
 * An immutable version of {@link Extract}.
 *
 * @author Yinan Li
 */
public class ImmutableExtract extends Extract {

  public ImmutableExtract(SourceState state, TableType type, String namespace, String table) {
    super(state, type, namespace, table);
  }

  public ImmutableExtract(Extract extract) {
    super(extract);
  }

  @Override
  public void setFullTrue(long extractFullRunTime) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setPrimaryKeys(String... primaryKeyFieldName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setDeltaFields(String... deltaFieldName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setId(String id) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setProp(String key, Object value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public synchronized void appendToListProp(String key, String value) {
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

  public void overrideWith(Properties properties) {
    throw new UnsupportedOperationException();
  }

  public void overrideWith(State otherState) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setExtractId(String extractId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void addPrimaryKey(String... primaryKeyFieldName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void addDeltaField(String... deltaFieldName) {
    throw new UnsupportedOperationException();
  }
}
