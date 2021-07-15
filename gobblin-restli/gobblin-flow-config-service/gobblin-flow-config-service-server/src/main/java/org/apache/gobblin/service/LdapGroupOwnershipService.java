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

import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;

import com.typesafe.config.Config;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.naming.NamingException;
import javax.naming.PartialResultException;

import org.apache.gobblin.annotation.Alias;
import org.apache.gobblin.util.LdapUtils;


/**
 * Queries external Active Directory service to check if the requester is part of the group
 */
@Alias("ldap")
@Singleton
public class LdapGroupOwnershipService extends GroupOwnershipService {
  LdapUtils ldapUtils;
  private static final Logger logger = Logger.getLogger(LdapGroupOwnershipService.class);

  @Inject
  public LdapGroupOwnershipService(Config config) {
    this.ldapUtils = new LdapUtils(config);
  }

  @Override
  public boolean isMemberOfGroup(List<ServiceRequester> serviceRequesters, String group) {
    try {
      Set<String> groupMemberships = ldapUtils.getGroupMembers(group);
      if (!groupMemberships.isEmpty()) {
        for (ServiceRequester requester: serviceRequesters) {
          if (groupMemberships.contains(requester.getName())) {
            return true;
          }
        }
      }
      return false;
    } catch (NamingException e) {
      logger.warn(String.format("Caught naming exception when parsing results from LDAP server. Message: %s",
          e.getExplanation()));
      if (e instanceof PartialResultException) {
        logger.warn("Check that the Ldap group exists");
        return false;
      }
      throw new RuntimeException(e);
    }
  }
}
