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

package gobblin.cluster;

import org.apache.helix.NotificationContext;
import org.apache.helix.messaging.handling.HelixTaskResult;
import org.apache.helix.messaging.handling.MessageHandler;
import org.apache.helix.messaging.handling.MessageHandlerFactory;
import org.apache.helix.model.Message;

import org.testng.Assert;


/**
 * A test implementation of {@link MessageHandlerFactory}.
 *
 * @author Yinan Li
 */
public class TestShutdownMessageHandlerFactory implements MessageHandlerFactory {

  private final HelixMessageTestBase helixMessageTestBase;

  public TestShutdownMessageHandlerFactory(HelixMessageTestBase helixMessageTestBase) {
    this.helixMessageTestBase = helixMessageTestBase;
  }

  @Override
  public MessageHandler createHandler(Message message, NotificationContext notificationContext) {
    return new TestShutdownMessageHandler(message, notificationContext, this.helixMessageTestBase);
  }

  @Override
  public String getMessageType() {
    return Message.MessageType.SHUTDOWN.toString();
  }

  @Override
  public void reset() {

  }

  private static class TestShutdownMessageHandler extends MessageHandler {

    private final HelixMessageTestBase helixMessageTestBase;

    public TestShutdownMessageHandler(Message message, NotificationContext context,
        HelixMessageTestBase helixMessageTestBase) {
      super(message, context);
      this.helixMessageTestBase = helixMessageTestBase;
    }

    @Override
    public HelixTaskResult handleMessage()
        throws InterruptedException {
      // Delay handling the message so the ZooKeeper client sees the message
      Thread.sleep(1000);
      this.helixMessageTestBase.assertMessageReception(_message);
      HelixTaskResult result = new HelixTaskResult();
      result.setSuccess(true);
      return result;
    }

    @Override
    public void onError(Exception e, ErrorCode errorCode, ErrorType errorType) {
      Assert.fail();
    }
  }
}
