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

package org.apache.gobblin.source.jdbc;

import com.mockrunner.mock.jdbc.MockResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.SimpleDateFormat;


/**
 * A class that mocks the getTimestamp() behavior of a ResultSet that is returned by mysql-connector-8.
 * This class expects that all entries in the ResultSet are timestamps
 */
class MockTimestampResultSet extends MockResultSet {
  enum ZeroDateTimeBehavior {
    CONVERT_TO_NULL, ROUND, EXCEPTION
  };

  private ZeroDateTimeBehavior _behavior;
  MockTimestampResultSet(String id, String behavior) {
    super(id);
    this._behavior = ZeroDateTimeBehavior.EXCEPTION; // default behavior is to throw an exception on a zero timestamp
    if (behavior != null) {
      this._behavior = ZeroDateTimeBehavior.valueOf(behavior);
    }
  }

  private boolean isZeroTimestamp(String s) {
    return s.startsWith("0000-00-00 00:00:00");
  }

  // mimic the behavior of mysql-connector-8's getTimestamp().
  // see com.mysql.cj.result.AbstractDateTimeValueFactory for details.
  @Override
  public Timestamp getTimestamp(int columnIndex) throws SQLException {
    Object obj = getObject(columnIndex);
    if (isZeroTimestamp(obj.toString())) {
      switch (_behavior) {
        case ROUND:
          return Timestamp.valueOf("0001-01-01 00:00:00.0");
        case CONVERT_TO_NULL:
          return null;
        case EXCEPTION:
          return Timestamp.valueOf(obj.toString()); // this throws an exception since timestamps cannot be zero in Java.
      }
    }

    return super.getTimestamp(columnIndex);
  }

  // mimic the behavior of mysql-connector-8's getString()
  // for timestamps, getString() formats the timestamp to "yyyy-MM-dd HH:mm:ss".
  @Override
  public String getString(int columnIndex) throws SQLException {
    if (this.getMetaData().getColumnType(columnIndex) == Types.TIMESTAMP) {
      if (isZeroTimestamp(getObject(columnIndex).toString())) {
        return "0000-00-00 00:00:00";
      }
      return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(getTimestamp(columnIndex));
    }
    return super.getString(columnIndex);
  }
}
