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

import com.typesafe.config.Config;

import gobblin.configuration.State;
import gobblin.util.ConfigUtils;
import gobblin.writer.DataWriter;
import gobblin.writer.DataWriterBuilder;




public class CouchbaseWriterBuilder extends DataWriterBuilder {
  @Override
  public DataWriter build()
      throws IOException {
    State state = this.destination.getProperties();
    Properties taskProps = state.getProperties();
    Config config = ConfigUtils.propertiesToConfig(taskProps);

    return new CouchbaseWriter(config);
  }
}
