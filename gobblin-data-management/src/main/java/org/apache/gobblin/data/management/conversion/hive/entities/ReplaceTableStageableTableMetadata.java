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

package org.apache.gobblin.data.management.conversion.hive.entities;

import org.apache.hadoop.hive.ql.metadata.Table;


/**
 * A {@link StageableTableMetadata} intended where the target table is the same as the reference table. Intended to
 * replace the original table.
 */
public class ReplaceTableStageableTableMetadata extends TableLikeStageableTableMetadata {

  public ReplaceTableStageableTableMetadata(Table referenceTable) {
    super(referenceTable, referenceTable.getDbName(), referenceTable.getTableName(), referenceTable.getDataLocation().toString());
  }

}
