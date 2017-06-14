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

package gobblin.r2;

import java.util.Map;

import com.linkedin.common.callback.Callback;
import com.linkedin.common.callback.FutureCallback;
import com.linkedin.common.util.None;
import com.linkedin.r2.transport.common.TransportClientFactory;
import com.linkedin.r2.transport.common.bridge.client.TransportClientAdapter;


/**
 * The proxy takes care of {@link TransportClientFactory} shutdown
 */
public class R2HttpClientProxy extends TransportClientAdapter {
  private final TransportClientFactory factory;

  public R2HttpClientProxy(TransportClientFactory factory, Map<String, Object> properties) {
    super(factory.getClient(properties));
    this.factory = factory;
  }

  @Override
  public void shutdown(Callback<None> callback) {
    try {
      super.shutdown(callback);
    } finally {
      factory.shutdown(new FutureCallback<>());
    }
  }
}
