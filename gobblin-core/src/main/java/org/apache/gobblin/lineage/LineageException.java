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


package org.apache.gobblin.lineage;

/**
 * A set of exceptions used by {@link LineageInfo} when lineage information is serialized or deserialized.
 */
public class LineageException extends Exception {
  public LineageException(String message) {
    super(message);
  }
  public static class LineageConflictAttributeException extends LineageException {
    public LineageConflictAttributeException (String key, String oldValue, String newValue) {
      super ("Lineage has conflict value: key=" + key + " value=[1]" + oldValue + " [2]" + newValue);
    }
  }

  public static class LineageUnsupportedLevelException extends LineageException {
    public LineageUnsupportedLevelException (LineageInfo.Level level) {
      super (level.toString() + " is not supported");
    }
  }
}
