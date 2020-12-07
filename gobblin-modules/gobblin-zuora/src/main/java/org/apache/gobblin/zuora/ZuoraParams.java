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
import java.util.List;

import org.apache.gobblin.annotation.Alpha;


@Alpha
public class ZuoraParams implements Serializable {
  private static final long serialVersionUID = 1L;

  String name;
  String partner;
  String project;
  List<ZuoraQuery> queries;
  String format;
  String version;
  String encrypted = "none";
  String useQueryLabels = "false";
  String dateTimeUtc = "true";

  ZuoraParams(String partner, String project, List<ZuoraQuery> queries, String name, String format, String version) {
    super();
    this.partner = partner;
    this.project = project;
    this.queries = queries;
    this.name = name;
    this.format = format;
    this.version = version;
  }

  public String getFormat() {
    return format;
  }

  public void setFormat(String format) {
    this.format = format;
  }

  public String getVersion() {
    return version;
  }

  public void setVersion(String version) {
    this.version = version;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getEncrypted() {
    return encrypted;
  }

  public void setEncrypted(String encrypted) {
    this.encrypted = encrypted;
  }

  public String getUseQueryLabels() {
    return useQueryLabels;
  }

  public void setUseQueryLabels(String useQueryLabels) {
    this.useQueryLabels = useQueryLabels;
  }

  public String getPartner() {
    return partner;
  }

  public void setPartner(String partner) {
    this.partner = partner;
  }

  public String getProject() {
    return project;
  }

  public void setProject(String project) {
    this.project = project;
  }

  public String getDateTimeUtc() {
    return dateTimeUtc;
  }

  public void setDateTimeUtc(String dateTimeUtc) {
    this.dateTimeUtc = dateTimeUtc;
  }

  public List<ZuoraQuery> getQueries() {
    return queries;
  }

  public void setQueries(List<ZuoraQuery> queries) {
    this.queries = queries;
  }

}
