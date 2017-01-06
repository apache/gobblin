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

package gobblin.cluster;

import org.apache.helix.messaging.AsyncCallback;
import org.apache.helix.model.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Class that handles Helix messaging response via no-op.
 *
 * @author Abhishek Tiwari
 */
public class NoopReplyHandler extends AsyncCallback {
  private static final Logger LOGGER = LoggerFactory.getLogger(NoopReplyHandler.class);

  private String bootstrapUrl;
  private String bootstrapTime;

  public NoopReplyHandler() {
  }

  public void onTimeOut() {
    LOGGER.error("Timed out");
  }

  public void onReplyMessage(Message message) {
    LOGGER.info("Received reply: " + message);
  }
}
