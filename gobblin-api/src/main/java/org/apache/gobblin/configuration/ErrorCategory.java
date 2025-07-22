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

package org.apache.gobblin.configuration;

import java.io.Serializable;
import lombok.Getter;
import lombok.Setter;

/*
 * Represents an error category used to classify errors within the Gobblin framework.
 * Each category has a name and a priority, which can be used to determine the severity
 * or importance of the error. This class is primarily used for error categorisation and handling.
 */
@Getter
@Setter
public class ErrorCategory implements Serializable {
  private String categoryName;
  private int priority;

  public ErrorCategory(String categoryName, int priority) {
    this.categoryName = categoryName;
    this.priority = priority;
  }

}

