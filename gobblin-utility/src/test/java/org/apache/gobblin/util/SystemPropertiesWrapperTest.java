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

import org.testng.annotations.Test;


public class SystemPropertiesWrapperTest {

  @Test
  public void testGetJavaHome() {
    final SystemPropertiesWrapper propertiesWrapper = new SystemPropertiesWrapper();
    final String home = propertiesWrapper.getJavaHome();
    // It's hard to assert where the java JRE home directory is used to launch this process.
    // This test is designed to print out the actual value for debugging and demonstration
    // purposes.
    System.out.println(home);
  }
}
