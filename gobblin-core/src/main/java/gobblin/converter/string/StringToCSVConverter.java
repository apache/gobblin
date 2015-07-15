/* (c) 2015 NerdWallet All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */
package gobblin.converter.string;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.WorkUnitState;
import gobblin.converter.Converter;
import gobblin.converter.DataConversionException;
import gobblin.converter.SchemaConversionException;
import gobblin.converter.SingleRecordIterable;
import gobblin.source.extractor.utils.InputStreamCSVReader;
import java.io.IOException;
import java.util.ArrayList;


/**
 * Parses a string as a CSV (using the delimiter set in {@link ConfigurationKeys#CONVERTER_STRING_SPLITTER_DELIMITER}
 * to an array of strings.
 *
 * @author ahollenbach@nerdwallet.com
 */
public class StringToCSVConverter extends Converter<Class<String>, Class<String>, String, ArrayList<String>> {
  @Override
  public Class<String> convertSchema(Class<String> inputSchema, WorkUnitState workUnit)
      throws SchemaConversionException {
    return String.class;
  }

  @Override
  public Iterable<ArrayList<String>> convertRecord(Class<String> outputSchema, String inputRecord,
      WorkUnitState workUnit) throws DataConversionException {
    char delimiter = workUnit.getProp(ConfigurationKeys.CONVERTER_CSV_DELIMITER,
        ConfigurationKeys.DEFAULT_CONVERTER_CSV_DELIMITER).charAt(0);

    try {
      return new SingleRecordIterable<ArrayList<String>>(splitString(inputRecord, delimiter));
    } catch (IOException e) {
      e.printStackTrace();
      throw new DataConversionException("Error splitting record");
    }
  }

  /**
   * Splits the input string using the given delimiter.
   * @param s The string to split
   * @param delimiter The delimiter to split on
   * @return An ArrayList of split strings using standard CSV splitting practices (respects quotes)
   * @throws IOException
   */
  public static ArrayList<String> splitString(String s, char delimiter)
      throws IOException {
    InputStreamCSVReader r = new InputStreamCSVReader(s, delimiter);
    return r.splitRecord();
  }
}