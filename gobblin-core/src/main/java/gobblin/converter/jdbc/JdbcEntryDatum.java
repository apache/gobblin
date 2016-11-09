/*
 * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.converter.jdbc;

import lombok.EqualsAndHashCode;
import lombok.ToString;

import com.google.common.base.Preconditions;


@ToString
@EqualsAndHashCode(of = { "columnName" })
public class JdbcEntryDatum {
  private final String columnName;
  private final Object val;

  public JdbcEntryDatum(String columnName, Object val) {
    this.columnName = Preconditions.checkNotNull(columnName);
    this.val = val;
  }

  public String getColumnName() {
    return this.columnName;
  }

  public Object getVal() {
    return this.val;
  }
}
