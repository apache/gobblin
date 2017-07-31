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

import java.util.Properties;

import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;


/**
 * Test {@link ImmutableProperties}.
 */
public class TestImmutableProperties {

  ImmutableProperties props;

  @BeforeTest
  public void setUp() {
    Properties originalProps = new Properties();
    originalProps.setProperty("a", "1");
    originalProps.setProperty("b", "2");
    originalProps.setProperty("c", "3");
    props = new ImmutableProperties(originalProps);
  }

  @Test
  public void testGetMethods() {
    Assert.assertEquals(props.getProperty("a"), "1");
    Assert.assertEquals(props.get("b"), "2");
    Assert.assertEquals(props.getProperty("c", "4"), "3");
    Assert.assertEquals(props.getProperty("d", "default"), "default");
  }

  @Test(expectedExceptions = UnsupportedOperationException.class)
  public void testSetMethods() {
    props.setProperty("a", "2");
  }

  @Test(expectedExceptions = UnsupportedOperationException.class)
  public void testPutMethods() {
    props.put("b", "3");
  }

  @Test(expectedExceptions = UnsupportedOperationException.class)
  public void testRemoveMethods() {
    props.remove("c");
  }
}
