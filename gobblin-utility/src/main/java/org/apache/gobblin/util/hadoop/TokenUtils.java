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

package org.apache.gobblin.util.hadoop;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Master;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.mapreduce.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.mapreduce.server.jobtracker.JTConfig;
import org.apache.hadoop.mapreduce.v2.api.HSClientProtocol;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetDelegationTokenRequest;
import org.apache.hadoop.mapreduce.v2.jobhistory.JHAdminConfig;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.log4j.Logger;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import org.apache.gobblin.configuration.State;
import org.apache.thrift.TException;


/**
 * A utility class for obtain Hadoop tokens and Hive metastore tokens for Azkaban jobs.
 *
 * <p>
 *   This class is compatible with Hadoop 2.
 * </p>
 */
public class TokenUtils {

  private static final Logger LOG = Logger.getLogger(TokenUtils.class);

  private static final String USER_TO_PROXY = "tokens.user.to.proxy";
  private static final String KEYTAB_USER = "keytab.user";
  private static final String KEYTAB_LOCATION = "keytab.location";
  private static final String HADOOP_SECURITY_AUTHENTICATION = "hadoop.security.authentication";
  private static final String OTHER_NAMENODES = "other_namenodes";
  private static final String KERBEROS = "kerberos";
  private static final String YARN_RESOURCEMANAGER_PRINCIPAL = "yarn.resourcemanager.principal";
  private static final String YARN_RESOURCEMANAGER_ADDRESS = "yarn.resourcemanager.address";
  private static final String MAPRED_JOB_TRACKER = "mapred.job.tracker";
  private static final String MAPREDUCE_JOBTRACKER_ADDRESS = "mapreduce.jobtracker.address";
  private static final Pattern KEYTAB_USER_PATTERN = Pattern.compile(".*\\/.*@.*");
  private static final String KERBEROS_REALM = "kerberos.realm";

  /**
   * the key that will be used to set proper signature for each of the hcat token when multiple hcat
   * tokens are required to be fetched.
   */
  private static final String HIVE_TOKEN_SIGNATURE_KEY = "hive.metastore.token.signature";
  private static final String OTHER_HIVE_LOCATIONS = "other.hcatLocation";

  /**
   * Get Hadoop tokens (tokens for job history server, job tracker, hive and HDFS) using Kerberos keytab,
   * on behalf on a proxy user, and embed tokens into a {@link UserGroupInformation} as returned result.
   *
   * Note that when a super-user is fetching tokens for other users,
   * {@link #fetchHcatToken(String, HiveConf, String, IMetaStoreClient)} getDelegationToken} explicitly
   * contains a string parameter indicating proxy user, while other hadoop services requires impersonation first.
   *
   * @param state A {@link State} object that should contain properties.
   * @param tokenFile The file that will finally store materialized credentials.
   * @param ugi The {@link UserGroupInformation} that to be added with negotiated credentials.
   * @param targetUser The user to be impersonated as for fetching hadoop tokens.
   * @return A {@link UserGroupInformation} containing negotiated credentials.
   */
  public static UserGroupInformation getHadoopAndHiveTokens(final State state, File tokenFile, UserGroupInformation ugi,
      IMetaStoreClient client, String targetUser) throws IOException, InterruptedException {
    ugi.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        getHadoopTokens(state, tokenFile);
        return null;
      }
    });
    Credentials cred = Credentials.readTokenStorageFile(new Path(tokenFile.toURI()), new Configuration());
    getHiveToken(state, client, cred, targetUser, ugi);
    for (final Token<? extends TokenIdentifier> t : cred.getAllTokens()) {
      ugi.addToken(t);
    }
    return ugi;
  }

  /**
   * Get Hadoop tokens (tokens for job history server, job tracker and HDFS) using Kerberos keytab.
   *
   * @param state A {@link State} object that should contain property {@link #USER_TO_PROXY},
   * {@link #KEYTAB_USER} and {@link #KEYTAB_LOCATION}. To obtain tokens for
   * other namenodes, use property {@link #OTHER_NAMENODES} with comma separated HDFS URIs.
   * @return A {@link File} containing the negotiated credentials.
   */
  public static File getHadoopTokens(final State state, File tokenFile) throws IOException, InterruptedException {

    Preconditions.checkArgument(state.contains(KEYTAB_USER), "Missing required property " + KEYTAB_USER);
    Preconditions.checkArgument(state.contains(KEYTAB_LOCATION), "Missing required property " + KEYTAB_LOCATION);

    Configuration configuration = new Configuration();
    configuration.set(HADOOP_SECURITY_AUTHENTICATION, KERBEROS);
    UserGroupInformation.setConfiguration(configuration);
    UserGroupInformation.loginUserFromKeytab(obtainKerberosPrincipal(state), state.getProp(KEYTAB_LOCATION));

    final Optional<String> userToProxy = Strings.isNullOrEmpty(state.getProp(USER_TO_PROXY)) ? Optional.<String>absent()
        : Optional.fromNullable(state.getProp(USER_TO_PROXY));
    final Configuration conf = new Configuration();
    final Credentials cred = new Credentials();

    LOG.info("Getting tokens for " + userToProxy);

    getJhToken(conf, cred);
    getFsAndJtTokens(state, conf, userToProxy, cred);

    persistTokens(cred, tokenFile);
    return tokenFile;
  }

  /**
   * Obtain kerberos principal in a dynamic way, where the instance's value is determined by the hostname of the machine
   * that the job is currently running on.
   * It will be invoked when {@link #KEYTAB_USER} is not following pattern specified in {@link #KEYTAB_USER_PATTERN}.
   * @throws UnknownHostException
   */
  public static String obtainKerberosPrincipal(final State state) throws UnknownHostException {
    if (!state.getProp(KEYTAB_USER).matches(KEYTAB_USER_PATTERN.pattern())) {
      Preconditions.checkArgument(state.contains(KERBEROS_REALM));
      return state.getProp(KEYTAB_USER) + "/" + InetAddress.getLocalHost().getCanonicalHostName() + "@" + state.getProp(
          KERBEROS_REALM);
    } else {
      return state.getProp(KEYTAB_USER);
    }
  }

  /**
   *
   * @param userToProxy The user that hiveClient is impersonating as to fetch the delegation tokens.
   * @param ugi The {@link UserGroupInformation} that to be added with negotiated credentials.
   */
  private static void getHiveToken(final State state, IMetaStoreClient hiveClient, Credentials cred,
      final String userToProxy, UserGroupInformation ugi) {
    try {
      // Fetch and save the default hcat token.
      LOG.info("Pre-fetching default Hive MetaStore token from hive");
      HiveConf hiveConf = new HiveConf();

      Token<DelegationTokenIdentifier> hcatToken = fetchHcatToken(userToProxy, hiveConf, null, hiveClient);
      cred.addToken(hcatToken.getService(), hcatToken);
      ugi.addToken(hcatToken);

      // Fetch extra Hcat location user specified.
      final List<String> extraHcatLocations = state.getPropAsList(OTHER_HIVE_LOCATIONS);
      if (Collections.EMPTY_LIST != extraHcatLocations) {
        LOG.info("Need to pre-fetch extra metaStore tokens from hive.");

        // start to process the user inputs.
        for (final String thriftUrl : extraHcatLocations) {
          LOG.info("Pre-fetching metaStore token from : " + thriftUrl);

          hiveConf = new HiveConf();
          hiveConf.set(HiveConf.ConfVars.METASTOREURIS.varname, thriftUrl);
          hcatToken = fetchHcatToken(userToProxy, hiveConf, thriftUrl, hiveClient);
          cred.addToken(hcatToken.getService(), hcatToken);
          ugi.addToken(hcatToken);

          LOG.info("Successfully fetched token for:" + thriftUrl);
        }
      }
    } catch (final Throwable t) {
      final String message = "Failed to get hive metastore token." + t.getMessage() + t.getCause();
      LOG.error(message, t);
      throw new RuntimeException(message);
    }
  }

  /**
   * function to fetch hcat token as per the specified hive configuration and then store the token
   * in to the credential store specified .
   *
   * @param userToProxy String value indicating the name of the user the token will be fetched for.
   * @param hiveConf the configuration based off which the hive client will be initialized.
   */
  private static Token<DelegationTokenIdentifier> fetchHcatToken(final String userToProxy, final HiveConf hiveConf,
      final String tokenSignatureOverwrite, final IMetaStoreClient hiveClient)
      throws IOException, TException, InterruptedException {

    LOG.info(HiveConf.ConfVars.METASTOREURIS.varname + ": " + hiveConf.get(HiveConf.ConfVars.METASTOREURIS.varname));

    LOG.info(HiveConf.ConfVars.METASTORE_USE_THRIFT_SASL.varname + ": " + hiveConf.get(
        HiveConf.ConfVars.METASTORE_USE_THRIFT_SASL.varname));

    LOG.info(HiveConf.ConfVars.METASTORE_KERBEROS_PRINCIPAL.varname + ": " + hiveConf.get(
        HiveConf.ConfVars.METASTORE_KERBEROS_PRINCIPAL.varname));

    final Token<DelegationTokenIdentifier> hcatToken = new Token<>();

    LOG.info(UserGroupInformation.getCurrentUser().toString());

    hcatToken.decodeFromUrlString(
        hiveClient.getDelegationToken(userToProxy, UserGroupInformation.getLoginUser().getShortUserName()));

    // overwrite the value of the service property of the token if the signature
    // override is specified.
    // If the service field is set, do not overwrite that
    if (hcatToken.getService().getLength() <= 0 && tokenSignatureOverwrite != null
        && tokenSignatureOverwrite.trim().length() > 0) {
      hcatToken.setService(new Text(tokenSignatureOverwrite.trim().toLowerCase()));

      LOG.info(HIVE_TOKEN_SIGNATURE_KEY + ":" + (tokenSignatureOverwrite == null ? "" : tokenSignatureOverwrite));
    }

    LOG.info("Created hive metastore token.");
    LOG.info("Token kind: " + hcatToken.getKind());
    LOG.info("Token service: " + hcatToken.getService());
    return hcatToken;
  }

  private static void getJhToken(Configuration conf, Credentials cred) throws IOException {
    YarnRPC rpc = YarnRPC.create(conf);
    final String serviceAddr = conf.get(JHAdminConfig.MR_HISTORY_ADDRESS);

    LOG.debug("Connecting to HistoryServer at: " + serviceAddr);
    HSClientProtocol hsProxy =
        (HSClientProtocol) rpc.getProxy(HSClientProtocol.class, NetUtils.createSocketAddr(serviceAddr), conf);
    LOG.info("Pre-fetching JH token from job history server");

    Token<?> jhToken = null;
    try {
      jhToken = getDelegationTokenFromHS(hsProxy, conf);
    } catch (Exception exc) {
      throw new IOException("Failed to fetch JH token.", exc);
    }

    if (jhToken == null) {
      LOG.error("getDelegationTokenFromHS() returned null");
      throw new IOException("Unable to fetch JH token.");
    }

    LOG.info("Created JH token: " + jhToken.toString());
    LOG.info("Token kind: " + jhToken.getKind());
    LOG.info("Token id: " + Arrays.toString(jhToken.getIdentifier()));
    LOG.info("Token service: " + jhToken.getService());

    cred.addToken(jhToken.getService(), jhToken);
  }

  private static void getFsAndJtTokens(final State state, final Configuration conf, final Optional<String> userToProxy,
      final Credentials cred) throws IOException, InterruptedException {

    if (userToProxy.isPresent()) {
      UserGroupInformation.createProxyUser(userToProxy.get(), UserGroupInformation.getLoginUser())
          .doAs(new PrivilegedExceptionAction<Void>() {
            @Override
            public Void run() throws Exception {
              getFsAndJtTokensImpl(state, conf, cred);
              return null;
            }
          });
    } else {
      getFsAndJtTokensImpl(state, conf, cred);
    }
  }

  private static void getFsAndJtTokensImpl(final State state, final Configuration conf, final Credentials cred)
      throws IOException {
    getHdfsToken(conf, cred);
    if (state.contains(OTHER_NAMENODES)) {
      getOtherNamenodesToken(state.getPropAsList(OTHER_NAMENODES), conf, cred);
    }
    getJtToken(cred);
  }

  private static void getHdfsToken(Configuration conf, Credentials cred) throws IOException {
    FileSystem fs = FileSystem.get(conf);
    LOG.info("Getting DFS token from " + fs.getUri());
    Token<?> fsToken = fs.getDelegationToken(getMRTokenRenewerInternal(new JobConf()).toString());
    if (fsToken == null) {
      LOG.error("Failed to fetch DFS token for ");
      throw new IOException("Failed to fetch DFS token.");
    }
    LOG.info("Created DFS token: " + fsToken.toString());
    LOG.info("Token kind: " + fsToken.getKind());
    LOG.info("Token id: " + Arrays.toString(fsToken.getIdentifier()));
    LOG.info("Token service: " + fsToken.getService());

    cred.addToken(fsToken.getService(), fsToken);
  }

  private static void getOtherNamenodesToken(List<String> otherNamenodes, Configuration conf, Credentials cred)
      throws IOException {
    LOG.info(OTHER_NAMENODES + ": " + otherNamenodes);
    Path[] ps = new Path[otherNamenodes.size()];
    for (int i = 0; i < ps.length; i++) {
      ps[i] = new Path(otherNamenodes.get(i).trim());
    }
    TokenCache.obtainTokensForNamenodes(cred, ps, conf);
    LOG.info("Successfully fetched tokens for: " + otherNamenodes);
  }

  private static void getJtToken(Credentials cred) throws IOException {
    try {
      JobConf jobConf = new JobConf();
      JobClient jobClient = new JobClient(jobConf);
      LOG.info("Pre-fetching JT token from JobTracker");

      Token<DelegationTokenIdentifier> mrdt = jobClient.getDelegationToken(getMRTokenRenewerInternal(jobConf));
      if (mrdt == null) {
        LOG.error("Failed to fetch JT token");
        throw new IOException("Failed to fetch JT token.");
      }
      LOG.info("Created JT token: " + mrdt.toString());
      LOG.info("Token kind: " + mrdt.getKind());
      LOG.info("Token id: " + Arrays.toString(mrdt.getIdentifier()));
      LOG.info("Token service: " + mrdt.getService());
      cred.addToken(mrdt.getService(), mrdt);
    } catch (InterruptedException ie) {
      throw new IOException(ie);
    }
  }

  private static void persistTokens(Credentials cred, File tokenFile) throws IOException {
    try (FileOutputStream fos = new FileOutputStream(tokenFile); DataOutputStream dos = new DataOutputStream(fos)) {
      cred.writeTokenStorageToStream(dos);
    }
    LOG.info("Tokens loaded in " + tokenFile.getAbsolutePath());
  }

  private static Token<?> getDelegationTokenFromHS(HSClientProtocol hsProxy, Configuration conf) throws IOException {
    GetDelegationTokenRequest request =
        RecordFactoryProvider.getRecordFactory(null).newRecordInstance(GetDelegationTokenRequest.class);
    request.setRenewer(Master.getMasterPrincipal(conf));
    org.apache.hadoop.yarn.api.records.Token mrDelegationToken;
    mrDelegationToken = hsProxy.getDelegationToken(request).getDelegationToken();
    return ConverterUtils.convertFromYarn(mrDelegationToken, hsProxy.getConnectAddress());
  }

  private static Text getMRTokenRenewerInternal(JobConf jobConf) throws IOException {
    String servicePrincipal = jobConf.get(YARN_RESOURCEMANAGER_PRINCIPAL, jobConf.get(JTConfig.JT_USER_NAME));
    Text renewer;
    if (servicePrincipal != null) {
      String target = jobConf.get(YARN_RESOURCEMANAGER_ADDRESS, jobConf.get(MAPREDUCE_JOBTRACKER_ADDRESS));
      if (target == null) {
        target = jobConf.get(MAPRED_JOB_TRACKER);
      }

      String addr = NetUtils.createSocketAddr(target).getHostName();
      renewer = new Text(SecurityUtil.getServerPrincipal(servicePrincipal, addr));
    } else {
      // No security
      renewer = new Text("azkaban mr tokens");
    }

    return renewer;
  }
}
