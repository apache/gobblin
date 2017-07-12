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

package gobblin.source;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import gobblin.configuration.WorkUnitState;
import gobblin.source.extractor.watermark.TimestampWatermark;

import gobblin.source.jdbc.MysqlExtractor;
import gobblin.source.jdbc.SqlServerExtractor;


/**
 * Complementary tests for {@link TimestampWatermark}
 */
public class TimestampWatermarkTest {

  private static final long WATERMARK_VALUE = 20141029133015L;
  private static final String COLUMN = "my_column";
  private static final String OPERATOR = ">=";

  private TimestampWatermark tsWatermark;
  private final String watermarkFormat = "yyyyMMddHHmmss";
  private final WorkUnitState workunitState = new WorkUnitState();

  @BeforeClass
  public void setUpBeforeClass() throws Exception {
    this.tsWatermark = new TimestampWatermark(COLUMN, this.watermarkFormat);
    this.workunitState.setId("");
  }

  @Test
  public void testGetWatermarkConditionMySql() throws Exception {
    MysqlExtractor extractor = new MysqlExtractor(this.workunitState);
    Assert.assertEquals(this.tsWatermark.getWatermarkCondition(extractor, WATERMARK_VALUE, OPERATOR),
        COLUMN + " " + OPERATOR + " '2014-10-29 13:30:15'");
  }

  @Test
  public void testGetWatermarkConditionSqlServer() throws Exception {
    SqlServerExtractor extractor = new SqlServerExtractor(this.workunitState);
    Assert.assertEquals(this.tsWatermark.getWatermarkCondition(extractor, WATERMARK_VALUE, OPERATOR),
        COLUMN + " " + OPERATOR + " '2014-10-29 13:30:15'");
  }
}

