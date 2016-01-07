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

package gobblin.yarn;

import java.io.IOException;
import java.net.URL;
import java.util.Collection;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.token.Token;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import gobblin.util.ConfigUtils;


/**
 * Unit tests for {@link YarnHelixUtils}.
 *
 * @author Yinan Li
 */
@Test(groups = { "gobblin.yarn" })
public class YarnHelixUtilsTest {

  private Configuration configuration;
  private FileSystem fileSystem;
  private Path tokenFilePath;
  private Token<?> token;

  @BeforeClass
  public void setUp() throws IOException {
    this.configuration = new Configuration();
    this.fileSystem = FileSystem.getLocal(this.configuration);
    this.tokenFilePath = new Path(YarnHelixUtilsTest.class.getSimpleName(), "token");
    this.token = new Token<>();
    this.token.setKind(new Text("test"));
    this.token.setService(new Text("test"));
  }

  @Test
  public void testConfigToProperties() {
    URL url = YarnHelixUtilsTest.class.getClassLoader().getResource(YarnHelixUtilsTest.class.getSimpleName() + ".conf");
    Assert.assertNotNull(url, "Could not find resource " + url);

    Config config = ConfigFactory.parseURL(url).resolve();
    Assert.assertEquals(config.getString("k1"), "v1");
    Assert.assertEquals(config.getString("k2"), "v1");
    Assert.assertEquals(config.getInt("k3"), 1000);
    Assert.assertTrue(config.getBoolean("k4"));
    Assert.assertEquals(config.getLong("k5"), 10000);

    Properties properties = ConfigUtils.configToProperties(config);
    Assert.assertEquals(properties.getProperty("k1"), "v1");
    Assert.assertEquals(properties.getProperty("k2"), "v1");
    Assert.assertEquals(properties.getProperty("k3"), "1000");
    Assert.assertEquals(properties.getProperty("k4"), "true");
    Assert.assertEquals(properties.getProperty("k5"), "10000");
  }

  @Test
  public void testWriteTokenToFile() throws IOException {
    YarnHelixUtils.writeTokenToFile(this.token, this.tokenFilePath, this.configuration);
  }

  @Test(dependsOnMethods = "testWriteTokenToFile")
  public void testReadTokenFromFile() throws IOException {
    Collection<Token<?>> tokens = YarnHelixUtils.readTokensFromFile(this.tokenFilePath, this.configuration);
    Assert.assertEquals(tokens.size(), 1);
    Token<?> token = tokens.iterator().next();
    Assert.assertEquals(token, this.token);
  }

  @AfterClass
  public void tearDown() throws IOException {
    if (this.fileSystem.exists(this.tokenFilePath.getParent())) {
      this.fileSystem.delete(this.tokenFilePath.getParent(), true);
    }
  }
}
