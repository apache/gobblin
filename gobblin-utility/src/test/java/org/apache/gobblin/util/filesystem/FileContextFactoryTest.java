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

package org.apache.gobblin.util.filesystem;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.testng.Assert;
import org.testng.annotations.Test;


public class FileContextFactoryTest {

  @Test
  public void test() throws IOException {
    Configuration con1 = new Configuration();
    Configuration con2 = new Configuration();
    con2.set("mykey", "myval");

    FileContext fc1 = FileContextFactory.getInstance((URI.create("file:///")), con1);
    FileContext fc2 = FileContextFactory.getInstance((URI.create("file:///")), con1);
    FileContext fc3 = FileContextFactory.getInstance((URI.create("file:///")), con2);
    FileContext fc4 = FileContextFactory.getInstance((URI.create("hdfs://localhost")), con1);

    Assert.assertEquals(fc1, fc2);
    Assert.assertEquals(fc1, fc3);  // configuration is not checked while caching the object
    Assert.assertNotEquals(fc1, fc4);
  }
}
