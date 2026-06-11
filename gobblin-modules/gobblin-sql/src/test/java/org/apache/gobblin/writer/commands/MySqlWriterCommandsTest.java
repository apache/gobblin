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

package org.apache.gobblin.writer.commands;

import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Unit tests for {@link MySqlWriterCommands#grantCoversDrop(String, String)}, the heuristic used by
 * {@code hasDropPrivilege} to decide (from {@code SHOW GRANTS} output) whether the current user can
 * DROP tables in a given database. The parse is intentionally conservative; these cases pin the
 * common grant shapes.
 */
@Test(groups = { "gobblin.writer" })
public class MySqlWriterCommandsTest {

  private static final String DB = "gobblin";

  public void detectsExplicitDropOnDatabase() {
    Assert.assertTrue(MySqlWriterCommands.grantCoversDrop(
        "GRANT SELECT, INSERT, CREATE, DROP ON `gobblin`.* TO `u`@`%`", DB));
  }

  public void detectsAllPrivilegesGlobal() {
    Assert.assertTrue(MySqlWriterCommands.grantCoversDrop(
        "GRANT ALL PRIVILEGES ON *.* TO `u`@`%` WITH GRANT OPTION", DB));
  }

  public void detectsAllPrivilegesOnDatabase() {
    Assert.assertTrue(MySqlWriterCommands.grantCoversDrop(
        "GRANT ALL PRIVILEGES ON `gobblin`.* TO `u`@`%`", DB));
  }

  public void matchesDatabaseCaseInsensitively() {
    Assert.assertTrue(MySqlWriterCommands.grantCoversDrop(
        "GRANT DROP ON `GOBBLIN`.* TO `u`@`%`", DB));
  }

  public void falseWhenNoDropPrivilege() {
    Assert.assertFalse(MySqlWriterCommands.grantCoversDrop(
        "GRANT SELECT, INSERT, UPDATE, CREATE ON `gobblin`.* TO `u`@`%`", DB));
  }

  public void falseForUsageGrant() {
    Assert.assertFalse(MySqlWriterCommands.grantCoversDrop(
        "GRANT USAGE ON *.* TO `u`@`%`", DB));
  }

  public void falseWhenDropIsForDifferentDatabase() {
    Assert.assertFalse(MySqlWriterCommands.grantCoversDrop(
        "GRANT DROP ON `other`.* TO `u`@`%`", DB));
  }

  public void dropInTableNameIsNotMistakenForPrivilege() {
    // DROP appears only as part of a table name (after ON), not as a granted privilege.
    Assert.assertFalse(MySqlWriterCommands.grantCoversDrop(
        "GRANT SELECT ON `gobblin`.`drop_audit` TO `u`@`%`", DB));
  }

  public void falseForNullGrantLine() {
    Assert.assertFalse(MySqlWriterCommands.grantCoversDrop(null, DB));
  }
}
