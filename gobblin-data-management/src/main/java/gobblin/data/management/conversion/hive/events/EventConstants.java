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
package gobblin.data.management.conversion.hive.events;

/**
 * Event names and metadata names used by hive conversion.
 */
public class EventConstants {

  public static final String CONVERSION_NAMESPACE = "gobblin.hive.conversion";
  public static final String CONVERSION_PREFIX = CONVERSION_NAMESPACE + ".";

  //Event names
  public static final String SETUP_EVENT = CONVERSION_PREFIX + "Setup";
  public static final String FIND_HIVE_TABLES_EVENT = CONVERSION_PREFIX + "FindHiveTables";
  public static final String CONVERSION_SUCCESSFUL_SLA_EVENT = CONVERSION_PREFIX + "ConversionSuccessful";
  public static final String CONVERSION_FAILED_EVENT = CONVERSION_PREFIX + "ConversionFailed";

}
