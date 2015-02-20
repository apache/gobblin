/* (c) 2014 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.salesforce;

public class SalesforceConfigurationKeys {
  public static final String SOURCE_QUERYBASED_SALESFORCE_IS_SOFT_DELETES_PULL_DISABLED =
      "source.querybased.salesforce.is.soft.deletes.pull.disabled";
  public static final int DEFAULT_SALESFORCE_MAX_CHARS_IN_FILE = 200000000;
  public static final int DEFAULT_SALESFORCE_MAX_ROWS_IN_FILE = 1000000;
}
