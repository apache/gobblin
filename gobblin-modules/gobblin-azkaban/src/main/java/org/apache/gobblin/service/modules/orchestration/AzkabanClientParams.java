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

package org.apache.gobblin.service.modules.orchestration;

/**
 * A collection of attributes used by {@link AzkabanClient} to form an HTTP request,
 * and parse the HTTP response. More details can be found at
 * {@linktourl https://azkaban.github.io/azkaban/docs/latest/#ajax-api}
 */
public class AzkabanClientParams {
  public static final String ACTION = "action";
  public static final String USERNAME = "username";
  public static final String PASSWORD = "password";
  public static final String SESSION_ID = "session.id";

  public static final String NAME = "name";
  public static final String DELETE = "delete";
  public static final String DESCRIPTION = "description";
  public static final String PROJECT = "project";
  public static final String FLOW = "flow";
  public static final String AJAX = "ajax";
  public static final String CONCURRENT_OPTION = "concurrentOption";
  public static final String MESSAGE = "message";
  public static final String STATUS = "status";
  public static final String ERROR = "error";
  public static final String EXECID = "execid";
  public static final String JOBID = "jobId";
  public static final String DATA = "data";
  public static final String OFFSET = "offset";
  public static final String LENGTH = "length";
}
