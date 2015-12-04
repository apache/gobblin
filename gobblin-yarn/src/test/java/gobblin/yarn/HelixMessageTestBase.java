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

import org.apache.helix.model.Message;


/**
 * An interface for test classes that involve sending and receiving Helix messages.
 *
 * @author ynli
 */
interface HelixMessageTestBase {

  /**
   * Assert the reception of a message.
   *
   * @param message the message to assert reception for
   */
  void assertMessageReception(Message message);
}
