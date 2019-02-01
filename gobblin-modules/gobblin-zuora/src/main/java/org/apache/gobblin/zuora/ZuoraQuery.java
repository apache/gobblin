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

package org.apache.gobblin.zuora;

import java.io.Serializable;

import org.apache.gobblin.annotation.Alpha;

import com.google.common.base.Strings;


@Alpha
public class ZuoraQuery implements Serializable {
  private static final long serialVersionUID = 1L;
  public String name;
  public String query;
  public String type = "zoqlexport";
  //Check the documentation here:
  //https://knowledgecenter.zuora.com/DC_Developers/T_Aggregate_Query_API/BA_Stateless_and_Stateful_Modes
  public ZuoraDeletedColumn deleted = null;

  ZuoraQuery(String name, String query, String deleteColumn) {
    super();
    this.name = name;
    this.query = query;
    if (!Strings.isNullOrEmpty(deleteColumn)) {
      deleted = new ZuoraDeletedColumn(deleteColumn);
    }
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getQuery() {
    return query;
  }

  public void setQuery(String query) {
    this.query = query;
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }
}
