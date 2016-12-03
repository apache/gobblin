/*
 * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.kafka.schemareg;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;


/**
 * An implementation of {@link BasePooledObjectFactory} for {@link HttpClient}.
 *
 * @author mitu
 */
public class HttpClientFactory extends BasePooledObjectFactory<HttpClient>{

  @Override
  public HttpClient create() throws Exception {
    return new HttpClient();
  }

  @Override
  public PooledObject<HttpClient> wrap(HttpClient obj) {
    return new DefaultPooledObject<>(obj);
  }

}
