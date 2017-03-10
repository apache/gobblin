package gobblin.crypto;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.testng.annotations.Test;

import gobblin.codec.StreamCodec;


public class GobblinEncryptionProviderTest {
  @Test
  public void testCanBuildAes() throws IOException {
    Map<String, Object> properties = new HashMap<>();
    properties.put(EncryptionConfigParser.ENCRYPTION_ALGORITHM_KEY, "aes_rotating");
    properties.put(EncryptionConfigParser.ENCRYPTION_KEYSTORE_PATH_KEY, getClass().getResource("/encryption_provider_test_keystore").toString());
    properties.put(EncryptionConfigParser.ENCRYPTION_KEYSTORE_PASSWORD_KEY, "abcd");

    StreamCodec c = EncryptionFactory.buildStreamCryptoProvider(properties);
    Assert.assertNotNull(c);

    byte[] toEncrypt = "Hello!".getBytes(StandardCharsets.UTF_8);

    ByteArrayOutputStream cipherOut = new ByteArrayOutputStream();
    OutputStream cipherStream = c.encodeOutputStream(cipherOut);
    cipherStream.write(toEncrypt);
    cipherStream.close();

    Assert.assertTrue("Expected to be able to write ciphertext!", cipherOut.size() > 0);
  }
}
