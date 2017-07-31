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

package org.apache.gobblin.commit;

import org.apache.gobblin.annotation.Alpha;


/**
 * A declaration by any Gobblin construct to claim whether it is safe to have multiple speculative attempts.
 * For example, if any {@link org.apache.gobblin.writer.DataWriter} implements {@link SpeculativeAttemptAwareConstruct}
 * and returns true in {@link #isSpeculativeAttemptSafe()}, then multiple attempts of one {@link org.apache.gobblin.writer.DataWriter}
 * should not cause conflict among them.
 */
@Alpha
public interface SpeculativeAttemptAwareConstruct {

  /**
   * @return true if it is safe to have multiple speculative attempts; false, otherwise.
   * To avoid inheritance issue, the suggested pattern would be "return this.class == MyClass.class".
   */
  public boolean isSpeculativeAttemptSafe();
}
