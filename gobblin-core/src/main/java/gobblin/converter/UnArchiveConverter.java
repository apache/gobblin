package gobblin.converter;

import gobblin.configuration.WorkUnitState;
import gobblin.data.management.copy.FileAwareInputStream;

import java.io.IOException;
import java.util.zip.GZIPInputStream;


public class UnArchiveConverter extends Converter<String, String, FileAwareInputStream, FileAwareInputStream> {

  @Override
  public String convertSchema(String inputSchema, WorkUnitState workUnit) throws SchemaConversionException {
    return inputSchema;
  }

  @Override
  public Iterable<FileAwareInputStream> convertRecord(String outputSchema, FileAwareInputStream fileAwareInputStream,
      WorkUnitState workUnit) throws DataConversionException {

    try {
      return new SingleRecordIterable<FileAwareInputStream>(new FileAwareInputStream(
          fileAwareInputStream.getFile(), new GZIPInputStream(fileAwareInputStream.getInputStream())));
    } catch (IOException e) {
      throw new DataConversionException(e);
    }

  }
}
