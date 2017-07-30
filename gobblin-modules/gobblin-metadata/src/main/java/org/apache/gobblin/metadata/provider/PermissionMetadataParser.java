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

package gobblin.metadata.provider;

import gobblin.metadata.types.GlobalMetadata;


/**
 * Parses permission information from a given {@link GlobalMetadata}.
 */
public class PermissionMetadataParser {

  private final static String PERMISSION_KEY = "Permission";
  private final static String GROUP_OWNER = "GroupOwner";

  public static void setPermission(GlobalMetadata metadata, String permission) {
    metadata.setDatasetMetadata(PERMISSION_KEY, permission);
  }

  public static String getPermission(GlobalMetadata metadata) {
    return (String) metadata.getDatasetMetadata(PERMISSION_KEY);
  }

  public static void setGroupOwner(GlobalMetadata metadata, String groupOwner) {
    metadata.setDatasetMetadata(GROUP_OWNER, groupOwner);
  }

  public static String getGroupOwner(GlobalMetadata metadata) {
    return (String) metadata.getDatasetMetadata(GROUP_OWNER);
  }
}
