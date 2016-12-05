/*
 *
 *  * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 *  * this file except in compliance with the License. You may obtain a copy of the
 *  * License at  http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software distributed
 *  * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 *  * CONDITIONS OF ANY KIND, either express or implied.
 *
 */

package gobblin.couchbase.writer;

import java.io.IOException;
import java.util.Properties;

import org.testng.annotations.Test;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;


/**
 * Created by sdas on 12/4/16.
 */
public class CouchbaseWriterTest {


  @Test
  public void testStringWrite()
      throws IOException {
    Properties props = new Properties();
    props.setProperty(CouchbaseWriterConfigurationKeys.BOOTSTRAP_SERVERS, "localhost");
    props.setProperty(CouchbaseWriterConfigurationKeys.BUCKET, "default");
    Config config = ConfigFactory.parseProperties(props);

    CouchbaseWriter writer = new CouchbaseWriter(config);
    writer.write(new KeyValueRecord<String, String>() {
      @Override
      public String getKey() {
        return "hello";
      }

      @Override
      public String getValue() {
        return "hello world";
      }
    });

  }

}
