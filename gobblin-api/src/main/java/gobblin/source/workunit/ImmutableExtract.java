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

import gobblin.configuration.SourceState;

/***
 * Shim layer for org.apache.gobblin.source.workunit.ImmutableExtract
 */
public class ImmutableExtract extends org.apache.gobblin.source.workunit.ImmutableExtract {

  public ImmutableExtract(SourceState state, gobblin.source.workunit.Extract.TableType type, String namespace, String table) {
    super(state, adaptTableType(type), namespace, table);
  }

  public ImmutableExtract(Extract extract) {
    super(extract);
  }

  private static org.apache.gobblin.source.workunit.Extract.TableType adaptTableType(Extract.TableType type) {
    switch (type) {
      case SNAPSHOT_ONLY: return org.apache.gobblin.source.workunit.Extract.TableType.SNAPSHOT_ONLY;
      case SNAPSHOT_APPEND: return org.apache.gobblin.source.workunit.Extract.TableType.SNAPSHOT_APPEND;
      default: return org.apache.gobblin.source.workunit.Extract.TableType.APPEND_ONLY;
    }
  }
}
