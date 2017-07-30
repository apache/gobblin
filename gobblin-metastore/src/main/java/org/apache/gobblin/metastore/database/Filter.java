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

package gobblin.metastore.database;

import com.google.common.base.Preconditions;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import lombok.EqualsAndHashCode;
import lombok.Getter;


@EqualsAndHashCode
class Filter {
    public static final Filter MISSING = new Filter();

    @Getter
    private final String filter;

    @Getter
    private final List<String> values;

    private Filter() {
        this.filter = null;
        this.values = null;
    }

    public Filter(String filter, List<String> values) {
        Preconditions.checkNotNull(filter);
        Preconditions.checkNotNull(values);
        this.filter = filter;
        this.values = values;
    }

    @Override
    public String toString() {
        return filter;
    }

    public int addParameters(PreparedStatement statement, int index) throws SQLException {
        if (values != null) {
            for (String value : values) {
                statement.setString(index++, value);
            }
        }
        return index;
    }

    public boolean isPresent() {
        return this != MISSING;
    }
}
