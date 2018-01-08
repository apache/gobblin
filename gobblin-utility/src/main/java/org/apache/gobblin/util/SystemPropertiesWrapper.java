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

package org.apache.gobblin.util;

public class SystemPropertiesWrapper {
  private String get(final String name) {
    return System.getProperty(name);
  }

  /**
   * The path to the JRE that is used to run the program which does the lookup. It is not
   * related to JAVA_HOME.
   * e.g.
   * /Library/Java/JavaVirtualMachines/jdk1.8.0_40.jdk/Contents/Home/jre
   */
  public String getJavaHome() {
    return get("java.home");
  }

  public String getJavaClassPath() {
    return get("java.class.path");
  }
}
