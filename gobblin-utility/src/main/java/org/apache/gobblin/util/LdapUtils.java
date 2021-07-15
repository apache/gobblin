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

package org.apache.gobblin.util;

import com.typesafe.config.Config;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Set;
import javax.naming.Context;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;
import org.apache.gobblin.password.PasswordManager;
import org.apache.log4j.Logger;

/**
 * This is a utility class for accessing Active Directory.
 * Utility factory which returns an instance of {@link LdapUtils}
 */
public class LdapUtils {
  public static final String LDAP_PREFIX = "groupOwnershipService.ldap";
  public static final String LDAP_BASE_DN_KEY = LDAP_PREFIX + ".baseDn";
  public static final String LDAP_HOST_KEY = LDAP_PREFIX + ".host";
  public static final String LDAP_PORT_KEY = LDAP_PREFIX + ".port";
  public static final String LDAP_USER_KEY = LDAP_PREFIX + ".username";
  public static final String LDAP_PASSWORD_KEY = LDAP_PREFIX + ".password";
  public static final String LDAP_USE_SECURE_TRUSTMANAGER = LDAP_PREFIX + ".useSecureTrustManager";

  private static final Logger logger = Logger.getLogger(LdapUtils.class);

  private final String _ldapHost;
  private final String _ldapPort;
  private final String _ldapBaseDN;

  // Creds of headless account for searching LDAP
  private final String _ldapUser;
  private final String _ldapPassword;
  private final boolean _ldapUseSecureTrustManager;

  private final String _personSearchFilter = "(&(objectcategory=Person)(samaccountname=%s))";
  private final String _groupSearchFilter = "(&(objectcategory=Group)(cn=%s))";
  private final String _memberSearchFilter = "(&(objectcategory=Person)(memberof=%s))";

  private final String _distinguishedName = "distinguishedName";
  private final String _samAccount = "sAMAccountName";
  private final String _memberOf = "memberof";

  public LdapUtils(Config config) {
    PasswordManager passwordManager = PasswordManager.getInstance(ConfigUtils.configToState(config));
    String password = passwordManager.readPassword(config.getString(LDAP_PASSWORD_KEY));
    _ldapHost = config.getString(LDAP_HOST_KEY);
    _ldapPort = config.getString(LDAP_PORT_KEY);
    _ldapUser = config.getString(LDAP_USER_KEY);
    _ldapPassword = password;
    _ldapBaseDN = config.getString(LDAP_BASE_DN_KEY);
    if(config.hasPath(LDAP_USE_SECURE_TRUSTMANAGER)) {
      _ldapUseSecureTrustManager = config.getBoolean(LDAP_USE_SECURE_TRUSTMANAGER);
    } else {
      _ldapUseSecureTrustManager = false;
    }
  }

  /**
   * Returns DirContext for making LDAP call
   *
   * @param username The LDAP sAMAccountName
   * @param password The LDAP password
   * @throws NamingException
   */
  private DirContext getDirContext(String username, String password) throws NamingException {
    Hashtable<String, Object> env = new Hashtable<>();
    env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
    env.put(Context.PROVIDER_URL, String.format("ldaps://%s:%s", _ldapHost, _ldapPort));
    env.put(Context.SECURITY_AUTHENTICATION, "simple");
    env.put(Context.SECURITY_PROTOCOL, "ssl");
    env.put(Context.SECURITY_PRINCIPAL, username);
    env.put(Context.SECURITY_CREDENTIALS, password);

    if (_ldapUseSecureTrustManager) {
      env.put("java.naming.ldap.factory.socket", TrustManagerSecureSocketFactory.class.getCanonicalName());
    } else {
      env.put("java.naming.ldap.factory.socket", TrustManagerSocketFactory.class.getCanonicalName());
    }

    return new InitialDirContext(env);
  }

  /**
   * Returns LDAP SearchResult for given filter and ctx
   *
   * @param searchFilter The LDAP filter
   * @param ctx The DirContext for LDAP
   * @throws NamingException
   */
  private NamingEnumeration<SearchResult> searchLDAP(String searchFilter, DirContext ctx) throws NamingException {
    String baseDN = _ldapBaseDN;
    SearchControls controls = new SearchControls();
    controls.setSearchScope(SearchControls.SUBTREE_SCOPE);
    return ctx.search(baseDN, searchFilter, controls);
  }

  /**
   * Returns String Attribute value
   *
   * @param result The LDAP SearchResult, could be either Person or Group
   * @param attribute Attribute to find from SearchResult
   * @throws NamingException
   */
  private String getAttribute(SearchResult result, String attribute) throws NamingException {
    return result.getAttributes().get(attribute).get().toString();
  }



  public Set<String> getGroupMembers(String groupName) throws NamingException {
    // Username and password for binding must exist
    if (_ldapUser == null || _ldapPassword == null) {
      throw new IllegalStateException("Username and password must be provided when initiating the class");
    }

    DirContext ctx;
    Set<String> resultSet = new HashSet<>();
    ctx = getDirContext(_ldapUser, _ldapPassword);
    logger.info("Searching for groups");
    String searchFilter = String.format(_groupSearchFilter, groupName);
    NamingEnumeration<SearchResult> groupResults = searchLDAP(searchFilter, ctx);
    SearchResult group = groupResults.next();
    String distinguishedName = getAttribute(group, _distinguishedName);
    String membersSearchFilter = String.format(_memberSearchFilter, distinguishedName);
    logger.info("Searching for members");
    NamingEnumeration<SearchResult> members = searchLDAP(membersSearchFilter, ctx);
    while (members.hasMoreElements()) {
      SearchResult member = members.next();
      resultSet.add(getAttribute(member, _samAccount));
    }
    logger.info(String.format("Members part of group %s: %s", groupName, resultSet.toString()));
    return resultSet;
  }
}
