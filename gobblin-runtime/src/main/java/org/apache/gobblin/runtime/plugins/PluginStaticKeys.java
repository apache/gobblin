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

package org.apache.gobblin.runtime.plugins;

/**
 * Static keys for {@link org.apache.gobblin.runtime.api.GobblinInstancePlugin}s.
 */
public class PluginStaticKeys {
  public static final String HADOOP_LOGIN_FROM_KEYTAB_ALIAS = "hadoopLoginFromKeytab";

  public static final String INSTANCE_CONFIG_PREFIX = "gobblin.instance";

  // Kerberos auth'n
  public static final String HADOOP_CONFIG_PREFIX = INSTANCE_CONFIG_PREFIX + ".hadoop";
  public static final String LOGIN_USER_KEY = "loginUser";
  public static final String LOGIN_USER_FULL_KEY = HADOOP_CONFIG_PREFIX + "." + LOGIN_USER_KEY;
  public static final String LOGIN_USER_KEYTAB_FILE_KEY = "loginUserKeytabFile";
  public static final String LOGIN_USER_KEYTAB_FILE_FULL_KEY =
      HADOOP_CONFIG_PREFIX + "." + LOGIN_USER_KEYTAB_FILE_KEY;
}
