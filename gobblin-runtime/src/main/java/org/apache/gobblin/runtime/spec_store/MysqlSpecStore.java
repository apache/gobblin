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

package org.apache.gobblin.runtime.spec_store;

import com.google.common.base.Charsets;
import com.google.common.io.ByteStreams;
import com.google.gson.Gson;
import com.typesafe.config.Config;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import lombok.extern.slf4j.Slf4j;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.runtime.api.FlowSpec;
import org.apache.gobblin.runtime.api.Spec;
import org.apache.gobblin.runtime.api.SpecSearchObject;
import org.apache.gobblin.runtime.api.SpecSerDe;
import org.apache.gobblin.runtime.api.SpecStore;
import org.apache.gobblin.service.ServiceConfigKeys;
import org.apache.gobblin.util.ConfigUtils;

import static org.apache.gobblin.service.ServiceConfigKeys.*;


/**
 * Implementation of {@link SpecStore} that stores specs in MySQL both as a serialized BLOB and as a JSON string, and extends
 * {@link MysqlBaseSpecStore} with enhanced FlowSpec search/retrieval capabilities.  The {@link SpecSerDe}'s serialized output
 * is presumed suitable for a MySql `JSON` column.  As in the base, versions are unsupported and ignored.
 *
 * ETYMOLOGY: this class might better be named `MysqlFlowSpecStore` while the base be `MysqlSpecStore`, but the `MysqlSpecStore` name's
 * association with this class's semantics predates the refactoring/generalization into `MysqlBaseSpecStore`.  Thus, to maintain
 * b/w-compatibility and avoid surprising legacy users who already refer to it (e.g. by name, in configuration), that original name remains intact.
 */
@Slf4j
public class MysqlSpecStore extends MysqlBaseSpecStore {
  public static final String CONFIG_PREFIX = "mysqlSpecStore";

  // Historical Note: the `spec_json` column didn't always exist and was introduced for GOBBLIN-1150; the impl. thus allows that not every
  // record may contain data there... though in practice, all should, given the passage of time (amidst the usual retention expiry).
  protected static final String SPECIFIC_INSERT_STATEMENT = "INSERT INTO %s (spec_uri, flow_group, flow_name, template_uri, "
      + "user_to_proxy, source_identifier, destination_identifier, schedule, tag, isRunImmediately, owning_group, spec, spec_json) "
      + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE spec = VALUES(spec), spec_json = VALUES(spec_json)";
  private static final String SPECIFIC_GET_STATEMENT_BASE = "SELECT spec_uri, spec, spec_json FROM %s WHERE ";
  private static final String SPECIFIC_GET_ALL_STATEMENT = "SELECT spec_uri, spec, spec_json FROM %s";
  private static final String SPECIFIC_CREATE_TABLE_STATEMENT = "CREATE TABLE IF NOT EXISTS %s (spec_uri VARCHAR("
      + FlowSpec.Utils.maxFlowSpecUriLength()
      + ") NOT NULL, flow_group VARCHAR(" + ServiceConfigKeys.MAX_FLOW_GROUP_LENGTH + "), flow_name VARCHAR("
      + ServiceConfigKeys.MAX_FLOW_GROUP_LENGTH + "), " + "template_uri VARCHAR(128), user_to_proxy VARCHAR(128), "
      + "source_identifier VARCHAR(128), " + "destination_identifier VARCHAR(128), schedule VARCHAR(128), "
      + "tag VARCHAR(128) NOT NULL, modified_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE "
      + "CURRENT_TIMESTAMP, isRunImmediately BOOLEAN, timezone VARCHAR(128), owning_group VARCHAR(128), spec LONGBLOB, "
      + "spec_json JSON, PRIMARY KEY (spec_uri))";

  /** Bundle all changes following from schema differences against the base class. */
  protected class SpecificSqlStatements extends SqlStatements {
    @Override
    public void completeInsertPreparedStatement(PreparedStatement statement, Spec spec, String tagValue) throws SQLException {
      FlowSpec flowSpec = (FlowSpec) spec;
      URI specUri = flowSpec.getUri();
      Config flowConfig = flowSpec.getConfig();
      String flowGroup = flowConfig.getString(ConfigurationKeys.FLOW_GROUP_KEY);
      String flowName = flowConfig.getString(ConfigurationKeys.FLOW_NAME_KEY);
      String templateURI = new Gson().toJson(flowSpec.getTemplateURIs());
      String userToProxy = ConfigUtils.getString(flowSpec.getConfig(), "user.to.proxy", null);
      String sourceIdentifier = flowConfig.getString(FLOW_SOURCE_IDENTIFIER_KEY);
      String destinationIdentifier = flowConfig.getString(FLOW_DESTINATION_IDENTIFIER_KEY);
      String schedule = ConfigUtils.getString(flowConfig, ConfigurationKeys.JOB_SCHEDULE_KEY, null);
      String owningGroup = ConfigUtils.getString(flowConfig, ConfigurationKeys.FLOW_OWNING_GROUP_KEY, null);
      boolean isRunImmediately = ConfigUtils.getBoolean(flowConfig, ConfigurationKeys.FLOW_RUN_IMMEDIATELY, false);

      int i = 0;
      statement.setString(++i, specUri.toString());
      statement.setString(++i, flowGroup);
      statement.setString(++i, flowName);
      statement.setString(++i, templateURI);
      statement.setString(++i, userToProxy);
      statement.setString(++i, sourceIdentifier);
      statement.setString(++i, destinationIdentifier);
      statement.setString(++i, schedule);
      statement.setString(++i, tagValue);
      statement.setBoolean(++i, isRunImmediately);
      statement.setString(++i, owningGroup);
      statement.setBlob(++i, new ByteArrayInputStream(MysqlSpecStore.this.specSerDe.serialize(flowSpec)));
      statement.setString(++i, new String(MysqlSpecStore.this.specSerDe.serialize(flowSpec), Charsets.UTF_8));
    }

    @Override
    public Spec extractSpec(ResultSet rs) throws SQLException, IOException {
      return rs.getString(3) == null
          ? MysqlSpecStore.this.specSerDe.deserialize(ByteStreams.toByteArray(rs.getBlob(2).getBinaryStream()))
          : MysqlSpecStore.this.specSerDe.deserialize(rs.getString(3).getBytes(Charsets.UTF_8));
    }

    @Override
    protected String getTablelessInsertStatement() { return MysqlSpecStore.SPECIFIC_INSERT_STATEMENT; }
    @Override
    protected String getTablelessGetStatementBase() { return MysqlSpecStore.SPECIFIC_GET_STATEMENT_BASE; }
    @Override
    protected String getTablelessGetAllStatement() { return MysqlSpecStore.SPECIFIC_GET_ALL_STATEMENT; }
    @Override
    protected String getTablelessCreateTableStatement() { return MysqlSpecStore.SPECIFIC_CREATE_TABLE_STATEMENT; }
  }


  public MysqlSpecStore(Config config, SpecSerDe specSerDe) throws IOException {
    super(config, specSerDe);
  }

  @Override
  protected String getConfigPrefix() {
    return MysqlSpecStore.CONFIG_PREFIX;
  }

  @Override
  protected SqlStatements createSqlStatements() {
    return new SpecificSqlStatements();
  }

  /** Support search, unlike base class (presumably via a {@link org.apache.gobblin.runtime.api.FlowSpecSearchObject}). */
  @Override
  public Collection<Spec> getSpecsImpl(SpecSearchObject specSearchObject) throws IOException {
    return withPreparedStatement(specSearchObject.augmentBaseGetStatement(this.sqlStatements.getStatementBase), statement -> {
      specSearchObject.completePreparedStatement(statement);
      return retrieveSpecs(statement);
    });
  }
}
