package gobblin.crypto;

import java.io.File;
import java.io.IOException;
import java.security.KeyStoreException;
import java.util.EnumSet;

import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;


public class KeystoreCredentialStoreTest {
  private File tempFile;

  @BeforeTest
  public void generateTempPath() throws IOException {
    tempFile = File.createTempFile("keystore_unit_test", null);
    tempFile.delete();
  }

  @AfterTest
  public void deleteTempPath() throws IOException {
    tempFile.delete();
  }

  @Test
  public void testGenerateKeys() throws KeyStoreException, IOException {
    final String path = tempFile.getAbsolutePath();
    final String password = "abcd";

    try {
      KeystoreCredentialStore cs = new KeystoreCredentialStore(path, password);
      Assert.fail("Expected exception to be thrown because keystore doesn't exist");
    } catch (IllegalArgumentException e) {
      // pass
    }

    KeystoreCredentialStore cs = new KeystoreCredentialStore(path, password, EnumSet.of(
        KeystoreCredentialStore.CreationOptions.CREATE_IF_MISSING));
    cs.generateAesKeys(20);

    cs = new KeystoreCredentialStore(path, password);
    Assert.assertEquals(cs.getAllEncodedKeys().size(), 20);
  }
}
