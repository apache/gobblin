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

package gobblin.data.management.copy.hive.avro;

import java.io.IOException;

import com.google.common.base.Optional;

import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.metadata.Partition;

import gobblin.data.management.copy.hive.HiveCopyEntityHelper;

/**
 * Update avro related entries in creating {@link CopyEntity}s for copying a Hive table.
 */

@Slf4j
public class HiveAvroCopyEntityHelper {
  private static final String HIVE_TABLE_AVRO_SCHEMA_URL  = "avro.schema.url";

  /**
   * Currently updated the {@link #HIVE_TABLE_AVRO_SCHEMA_URL} location for new hive table
   * @param targetTable, new Table to be registered in hive
   * @throws IOException
   */
  public static void updateAvroTableAttributes(Table targetTable, HiveCopyEntityHelper hiveHelper) throws IOException {
    if(!isHiveTableAvroType(targetTable)){
      return;
    }
    
    //Path targetLocation = targetTable.getDataLocation();
    
    // need to update the {@link #HIVE_TABLE_AVRO_SCHEMA_URL} location
    String oldAvroSchemaURL = targetTable.getTTable().getSd().getSerdeInfo().getParameters().get(HIVE_TABLE_AVRO_SCHEMA_URL);
    if(oldAvroSchemaURL!=null){
      //String newAvroSchemaURL = new Path(targetLocation, new Path(oldAvroSchemaURL).getName()).toString();
      String newAvroSchemaURL = hiveHelper.getTargetPath(new Path(oldAvroSchemaURL), hiveHelper.getTargetFileSystem(), 
          Optional.<Partition>absent(), true).toString();
      
      targetTable.getTTable().getSd().getSerdeInfo().getParameters().put(HIVE_TABLE_AVRO_SCHEMA_URL, newAvroSchemaURL);
      log.info(String.format("For table %s, change %s from %s to %s", targetTable.getCompleteName(), HIVE_TABLE_AVRO_SCHEMA_URL, oldAvroSchemaURL, newAvroSchemaURL));
    }
  }
  
  /**
   * Tell whether a hive table is actually an Avro table
   * @param targetTable
   * @return
   * @throws IOException
   */
  public static boolean isHiveTableAvroType(Table targetTable) throws IOException {
    String serializationLib = targetTable.getTTable().getSd().getSerdeInfo().getSerializationLib();
    String inputFormat = targetTable.getTTable().getSd().getInputFormat();
    String outputFormat = targetTable.getTTable().getSd().getOutputFormat();

    return inputFormat.endsWith("AvroContainerInputFormat") || outputFormat.endsWith("AvroContainerOutputFormat")
        || serializationLib.endsWith("AvroSerDe");
  }
}
