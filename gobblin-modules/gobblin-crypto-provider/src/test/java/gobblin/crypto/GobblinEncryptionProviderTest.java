package gobblin.crypto;

import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.testng.annotations.Test;

import gobblin.codec.StreamCodec;


public class GobblinEncryptionProviderTest {
  @Test
  public void testCanBuildAes() {
    Map<String, Object> properties = new HashMap<>();
    properties.put(EncryptionConfigParser.ENCRYPTION_ALGORITHM_KEY, "aes_rotating");
    properties.put(EncryptionConfigParser.ENCRYPTION_KEYSTORE_PATH_KEY, getClass().getResource("/encryption_provider_test_keystore").toString());
    properties.put(EncryptionConfigParser.ENCRYPTION_KEYSTORE_PASSWORD_KEY, "abcd");

    StreamCodec c = EncryptionFactory.buildStreamCryptoProvider(properties);
    Assert.assertNotNull(c);
  }
}
