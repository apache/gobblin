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
package gobblin.runtime.instance.plugin.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

import com.typesafe.config.Config;

import gobblin.annotation.Alias;
import gobblin.runtime.api.GobblinInstanceDriver;
import gobblin.runtime.instance.hadoop.HadoopConfigLoader;
import gobblin.runtime.instance.plugin.BaseIdlePluginImpl;

/**
 * Loads a Kerberos keytab file for Hadoop authentication.
 */
@Alias(value="hadoopLoginFromKeytab")
public class HadoopKerberosKeytabAuthenticationPlugin extends BaseIdlePluginImpl {
  public static final String INSTANCE_CONFIG_PREFIX = "gobblin.instance";
  public static final String CONFIG_PREFIX = INSTANCE_CONFIG_PREFIX + ".hadoop";
  public static final String LOGIN_USER_KEY = "loginUser";
  public static final String LOGIN_USER_KEYTAB_FILE_KEY = "loginUserKeytabFile";
  public static final String LOGIN_USER_FULL_KEY = CONFIG_PREFIX + "." + LOGIN_USER_KEY;
  public static final String LOGIN_USER_KEYTAB_FILE_FULL_KEY =
      CONFIG_PREFIX + "." + LOGIN_USER_KEYTAB_FILE_KEY;

  private final String _loginUser;
  private final String _loginUserKeytabFile;
  private final Configuration _hadoopConf;

  public HadoopKerberosKeytabAuthenticationPlugin(GobblinInstanceDriver instance) {
    super(instance);
    Config sysConfig = instance.getSysConfig().getConfig();
    if (!sysConfig.hasPath(LOGIN_USER_FULL_KEY)) {
      throw new RuntimeException("Missing required sys config: " + LOGIN_USER_FULL_KEY);
    }
    if (!sysConfig.hasPath(LOGIN_USER_KEYTAB_FILE_FULL_KEY)) {
      throw new RuntimeException("Missing required sys config: " + LOGIN_USER_KEYTAB_FILE_FULL_KEY);
    }

    _loginUser = sysConfig.getString(LOGIN_USER_FULL_KEY);
    _loginUserKeytabFile = sysConfig.getString(LOGIN_USER_KEYTAB_FILE_FULL_KEY);
    HadoopConfigLoader configLoader =  new HadoopConfigLoader(sysConfig);
    _hadoopConf = configLoader.getConf();
  }

  /** {@inheritDoc} */
  @Override
  protected void startUp() throws Exception {
    UserGroupInformation.setConfiguration(_hadoopConf);
    if (UserGroupInformation.isSecurityEnabled()) {
      UserGroupInformation.loginUserFromKeytab(_loginUser, _loginUserKeytabFile);
    }
  }

  public String getLoginUser() {
    return _loginUser;
  }

  public String getLoginUserKeytabFile() {
    return _loginUserKeytabFile;
  }

  public Configuration getHadoopConf() {
    return _hadoopConf;
  }


}
