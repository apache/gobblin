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

public class ServiceAzkabanConfigKeys {
  public static final String GOBBLIN_SERVICE_AZKABAN_PREFIX = "gobblin.service.azkaban.";

  // Azkaban Session Specifics
  public static final String AZKABAN_USERNAME_KEY = GOBBLIN_SERVICE_AZKABAN_PREFIX + "username";
  public static final String AZKABAN_PASSWORD_KEY = GOBBLIN_SERVICE_AZKABAN_PREFIX + "password";
  public static final String AZKABAN_SERVER_URL_KEY = GOBBLIN_SERVICE_AZKABAN_PREFIX + "server.url";
  public static final String AZKABAN_PROJECT_NAME_PREFIX_KEY = GOBBLIN_SERVICE_AZKABAN_PREFIX + "project.namePrefix";
  public static final String AZKABAN_PROJECT_DESCRIPTION_KEY = GOBBLIN_SERVICE_AZKABAN_PREFIX + "project.description";
  public static final String AZKABAN_PROJECT_USER_TO_PROXY_KEY = GOBBLIN_SERVICE_AZKABAN_PREFIX + "project.userToProxy";
  public static final String AZKABAN_PROJECT_FLOW_NAME_KEY = GOBBLIN_SERVICE_AZKABAN_PREFIX + "project.flowName";
  public static final String AZKABAN_PROJECT_GROUP_ADMINS_KEY = GOBBLIN_SERVICE_AZKABAN_PREFIX + "project.groupAdmins";
  public static final String AZKABAN_PROJECT_ZIP_JAR_URL_TEMPLATE_KEY = GOBBLIN_SERVICE_AZKABAN_PREFIX + "project.zip.jarUrlTemplate";
  public static final String AZKABAN_PROJECT_ZIP_JAR_NAMES_KEY = GOBBLIN_SERVICE_AZKABAN_PREFIX + "project.zip.jarNames";
  public static final String AZKABAN_PROJECT_ZIP_JAR_VERSION_KEY = GOBBLIN_SERVICE_AZKABAN_PREFIX + "project.zip.jarVersion";
  public static final String AZKABAN_PROJECT_ZIP_FAIL_IF_JARNOTFOUND_KEY = GOBBLIN_SERVICE_AZKABAN_PREFIX + "project.zip.failIfJarNotFound";
  public static final String AZKABAN_PROJECT_ZIP_ADDITIONAL_FILE_URLS_KEY = GOBBLIN_SERVICE_AZKABAN_PREFIX + "project.zip.additionalFilesUrl";
  public static final String AZKABAN_PROJECT_OVERWRITE_IF_EXISTS_KEY = GOBBLIN_SERVICE_AZKABAN_PREFIX + "project.overwriteIfExists";

  // Azkaban System Environment
  public static final String AZKABAN_PASSWORD_SYSTEM_KEY = "GOBBLIN_SERVICE_AZKABAN_PASSWORD";
  public static final String DEFAULT_AZKABAN_PROJECT_CONFIG_FILE = "default-service-azkaban.conf";
}

