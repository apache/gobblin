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
package org.apache.gobblin.compliance.utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.thrift.DelegationTokenIdentifier;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.thrift.TException;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.compliance.ComplianceConfigurationKeys;
import org.apache.gobblin.compliance.HiveProxyQueryExecutor;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.util.HostUtils;
import org.apache.gobblin.util.WriterUtils;


/**
 * A utility class for letting hadoop super user to proxy.
 *
 * @author adsharma
 */
@Slf4j
public class ProxyUtils {

  public static void setProxySettingsForFs(State state) {
    if (state.getPropAsBoolean(ComplianceConfigurationKeys.GOBBLIN_COMPLIANCE_SHOULD_PROXY,
        ComplianceConfigurationKeys.GOBBLIN_COMPLIANCE_DEFAULT_SHOULD_PROXY)) {
      String proxyUser = state.getProp(ComplianceConfigurationKeys.GOBBLIN_COMPLIANCE_PROXY_USER);
      String superUser = state.getProp(ComplianceConfigurationKeys.GOBBLIN_COMPLIANCE_SUPER_USER);
      String realm = state.getProp(ConfigurationKeys.KERBEROS_REALM);
      state.setProp(ConfigurationKeys.SHOULD_FS_PROXY_AS_USER, true);
      state.setProp(ConfigurationKeys.FS_PROXY_AS_USER_NAME, proxyUser);
      state.setProp(ConfigurationKeys.SUPER_USER_NAME_TO_PROXY_AS_OTHERS,
          HostUtils.getPrincipalUsingHostname(superUser, realm));
      state.setProp(ConfigurationKeys.FS_PROXY_AUTH_METHOD, ConfigurationKeys.KERBEROS_AUTH);
    }
  }

  public static void cancelTokens(State state)
      throws IOException, InterruptedException, TException {
    Preconditions.checkArgument(state.contains(ConfigurationKeys.SUPER_USER_KEY_TAB_LOCATION),
        "Missing required property " + ConfigurationKeys.SUPER_USER_KEY_TAB_LOCATION);
    Preconditions.checkArgument(state.contains(ComplianceConfigurationKeys.GOBBLIN_COMPLIANCE_SUPER_USER),
        "Missing required property " + ComplianceConfigurationKeys.GOBBLIN_COMPLIANCE_SUPER_USER);
    Preconditions.checkArgument(state.contains(ConfigurationKeys.KERBEROS_REALM),
        "Missing required property " + ConfigurationKeys.KERBEROS_REALM);

    String superUser = state.getProp(ComplianceConfigurationKeys.GOBBLIN_COMPLIANCE_SUPER_USER);
    String keytabLocation = state.getProp(ConfigurationKeys.SUPER_USER_KEY_TAB_LOCATION);
    String realm = state.getProp(ConfigurationKeys.KERBEROS_REALM);

    UserGroupInformation.loginUserFromKeytab(HostUtils.getPrincipalUsingHostname(superUser, realm), keytabLocation);
    UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
    UserGroupInformation realUser = currentUser.getRealUser();
    Credentials credentials = realUser.getCredentials();
    for (Token<?> token : credentials.getAllTokens()) {
      if (token.getKind().equals(DelegationTokenIdentifier.HIVE_DELEGATION_KIND)) {
        log.info("Cancelling hive token");
        HiveMetaStoreClient hiveClient = new HiveMetaStoreClient(new HiveConf());
        hiveClient.cancelDelegationToken(token.encodeToUrlString());
      }
    }
  }

  public static FileSystem getOwnerFs(State state, Optional<String> owner)
      throws IOException {
    if (owner.isPresent()) {
      state.setProp(ComplianceConfigurationKeys.GOBBLIN_COMPLIANCE_PROXY_USER, owner.get());
    }
    ProxyUtils.setProxySettingsForFs(state);
    return WriterUtils.getWriterFs(state);
  }

  @SafeVarargs
  public static HiveProxyQueryExecutor getQueryExecutor(State state, Optional<String>... owners)
      throws IOException {
    List<String> proxies = new ArrayList<>();
    for (Optional<String> owner : owners) {
      if (owner.isPresent()) {
        proxies.add(owner.get());
      }
    }
    return new HiveProxyQueryExecutor(state, proxies);
  }
}
