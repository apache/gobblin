package gobblin.converter;

import gobblin.configuration.WorkUnitState;
import gobblin.data.management.copy.FileAwareInputStream;
import gobblin.util.GPGFileDecrypter;

import java.io.IOException;
import java.security.NoSuchProviderException;

import org.bouncycastle.openpgp.PGPException;


public class DecryptConverter extends Converter<String, String, FileAwareInputStream, FileAwareInputStream> {

  @Override
  public String convertSchema(String inputSchema, WorkUnitState workUnit) throws SchemaConversionException {
    return inputSchema;
  }

  @Override
  public Iterable<FileAwareInputStream> convertRecord(String outputSchema, FileAwareInputStream fileAwareInputStream,
      WorkUnitState workUnit) throws DataConversionException {

    try {
      fileAwareInputStream.setInputStream(GPGFileDecrypter.decryptFile(fileAwareInputStream.getInputStream(), "X9k2Qktny9bACfvCrNpvqgEqMLo82cbX"));
      return new SingleRecordIterable<FileAwareInputStream>(fileAwareInputStream);
    } catch (IOException e) {
      throw new DataConversionException(e);
    } catch (NoSuchProviderException e) {
      throw new DataConversionException(e);
    } catch (PGPException e) {
      throw new DataConversionException(e);
    }

  }

}
