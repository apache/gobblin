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
package gobblin.converter.s3;

import gobblin.configuration.WorkUnitState;
import gobblin.converter.Converter;
import gobblin.converter.DataConversionException;
import gobblin.converter.SchemaConversionException;
import gobblin.converter.SingleRecordIterable;

import java.util.ArrayList;


/**
 * Converts an {@link ArrayList} of Strings coming from a space-separated value
 * into an {@link ELBRecord}.
 *
 * @author ahollenbach@nerdwallet.com
 */
public class CSVToELBRecordConverter extends
    Converter<Class<ArrayList<String>>, Class<ELBRecord>, ArrayList<String>, ELBRecord> {
  @Override
  public Class<ELBRecord> convertSchema(Class<ArrayList<String>> inputSchema, WorkUnitState workUnit)
      throws SchemaConversionException {
    return ELBRecord.class;
  }

  @Override
  public Iterable<ELBRecord>
  convertRecord(Class<ELBRecord> outputSchema, ArrayList<String> inputRecord, WorkUnitState workUnit)
      throws DataConversionException {
    return new SingleRecordIterable<ELBRecord>(new ELBRecord(inputRecord));
  }
}
