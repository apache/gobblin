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

package org.apache.gobblin.service;

import java.util.HashMap;
import java.util.Map;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

/**
 * A {@code ServiceRequester} represents who sends a request
 * via a rest api. The requester have multiple attributes.
 *
 * 'name' indicates who is the sender.
 * 'type' indicates if the sender is a user or a group or a specific application.
 * 'from' indicates where this sender information is extracted.
 *
 * Please note that 'name' should be unique for the same 'type' of requester(s).
 */
@Getter
@Setter
@EqualsAndHashCode
public class ServiceRequester {
  private String name;  // requester name
  private String type;  // requester can be user name, service name, group name, etc.
  private String from;  // the location or context where this requester info is obtained
  private Map<String, String> properties = new HashMap<>(); // additional information for future expansion

  /*
   * Default constructor is required for deserialization from json
   */
  public ServiceRequester() {
  }

  public ServiceRequester(String name, String type, String from) {
    this.name = name;
    this.type = type;
    this.from = from;
  }

  public String toString() {
    return "[name : " + name + " type : " + type + " from : "+ from + " additional : " + properties + "]";
  }
}
