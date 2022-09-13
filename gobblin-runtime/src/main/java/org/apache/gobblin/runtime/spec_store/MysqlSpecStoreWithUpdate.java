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
import com.typesafe.config.Config;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import org.apache.gobblin.runtime.api.FlowSpec;
import org.apache.gobblin.runtime.api.Spec;
import org.apache.gobblin.runtime.api.SpecSerDe;


public class MysqlSpecStoreWithUpdate extends MysqlSpecStore{
  // In this case, when we try to insert but key is existed, we will throw exception
  protected static final String INSERT_STATEMENT_WITHOUT_UPDATE = "INSERT INTO %s (spec_uri, flow_group, flow_name, template_uri, "
      + "user_to_proxy, source_identifier, destination_identifier, schedule, tag, isRunImmediately, owning_group, spec, spec_json) "
      + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
  public MysqlSpecStoreWithUpdate(Config config, SpecSerDe specSerDe) throws IOException {
    super(config, specSerDe);
  }

  /** Bundle all changes following from schema differences against the base class. */
  protected class SpecificSqlStatementsWithUpdate extends SpecificSqlStatements {
    public void completeUpdatePreparedStatement(PreparedStatement statement, Spec spec, long version) throws
                                                                                                         SQLException {
      FlowSpec flowSpec = (FlowSpec) spec;
      URI specUri = flowSpec.getUri();

      int i = 0;

      statement.setBlob(++i, new ByteArrayInputStream(MysqlSpecStoreWithUpdate.this.specSerDe.serialize(flowSpec)));
      statement.setString(++i, new String(MysqlSpecStoreWithUpdate.this.specSerDe.serialize(flowSpec), Charsets.UTF_8));
      statement.setString(++i, specUri.toString());
      statement.setLong(++i, version);
    }

    @Override
    protected String getTablelessInsertStatement() { return INSERT_STATEMENT_WITHOUT_UPDATE; }
  }

  @Override
  protected SqlStatements createSqlStatements() {
    return new SpecificSqlStatementsWithUpdate();
  }

  @Override
  // TODO: fix to obey the `SpecStore` contract of returning the *updated* `Spec`
  public Spec updateSpecImpl(Spec spec) throws IOException {
    updateSpecImpl(spec, Long.MAX_VALUE);
    return spec;
  }

  @Override
  // TODO: fix to obey the `SpecStore` contract of returning the *updated* `Spec`
  // Update {@link Spec} in the {@link SpecStore} when current version is smaller than {@link version}.
  // We use modified timestamp as the version here, that we only update entry where it's modified before a specific time.
  public Spec updateSpecImpl(Spec spec, long version) throws IOException {
    withPreparedStatement(this.sqlStatements.updateStatement, statement -> {
      ((SpecificSqlStatementsWithUpdate)this.sqlStatements).completeUpdatePreparedStatement(statement, spec, version);
      int i = statement.executeUpdate();
      if (i == 0) {
        throw new IOException("Spec does not exist or concurrent update happens, please check current spec and update again");
      }
      return null; // (type: `Void`)
    }, true);
    return spec;
  }



}
