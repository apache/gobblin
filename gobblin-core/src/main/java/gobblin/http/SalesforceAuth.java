package gobblin.http;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Properties;

import org.apache.oltu.oauth2.client.response.OAuthAccessTokenResponse;

import com.google.common.collect.ImmutableMap;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

import lombok.Getter;

import gobblin.configuration.ConfigurationKeys;
import gobblin.password.PasswordManager;


public class SalesforceAuth extends OAuthAccessToken {
  private static final Config FALLBACK = ConfigFactory.parseMap(ImmutableMap.<String, String>builder()
      .put(ApacheHttpClient.STATIC_SVC_ENDPOINT, "https://login.salesforce.com/services/oauth2/token")
      .put(SalesforceAuth.SECURITY_TOKEN, "").build());

  private static final String INSTANCE_URL = "instance_url";

  public static final String SFDC_PREFIX = "salesforce.";
  public static final String CLIENT_ID = SFDC_PREFIX + "client_id";
  public static final String CLIENT_SECRET = SFDC_PREFIX + "client_secret";
  public static final String USER_ID = SFDC_PREFIX + "user_id";
  public static final String PASSWORD = SFDC_PREFIX + "password";
  public static final String SFDC_ENCRYPT_KEY_LOC = SFDC_PREFIX + ConfigurationKeys.ENCRYPT_KEY_LOC;
  public static final String USE_STRONG_ENCRYPTION = SFDC_PREFIX + "strong_encryption";
  public static final String SECURITY_TOKEN = SFDC_PREFIX + "security_token";
  public static final String OAUTH_ENDPOINT = SFDC_PREFIX + "oauth_endpoint";

  @Getter
  private URI instanceUrl;

  public SalesforceAuth(String clientId, String clientSecret, String userId, String password, String securityToken,
      String oauthPath) {
    super(clientId, clientSecret, userId, password, securityToken, oauthPath);
  }

  public static SalesforceAuth create(Config config) {
    config = config.withValue(SalesforceAuth.OAUTH_ENDPOINT, config.getValue(ApacheHttpClient.STATIC_SVC_ENDPOINT));
    config = config.withFallback(FALLBACK);

    String clientId = config.getString(CLIENT_ID);
    String clientSecret = config.getString(CLIENT_SECRET);
    String userId = config.getString(USER_ID);
    String password = config.getString(PASSWORD);
    String securityToken = config.getString(SECURITY_TOKEN);

    if (config.hasPath(SFDC_ENCRYPT_KEY_LOC)) {
      Properties props = new Properties();
      if (config.hasPath(USE_STRONG_ENCRYPTION)) {
        props.put(ConfigurationKeys.ENCRYPT_USE_STRONG_ENCRYPTOR, config.getString(USE_STRONG_ENCRYPTION));
      }

      props.put(ConfigurationKeys.ENCRYPT_KEY_LOC, config.getString(SFDC_ENCRYPT_KEY_LOC));
      password = PasswordManager.getInstance(props).readPassword(password);
    }
    String oauthPath = config.getString(OAUTH_ENDPOINT);
    return new SalesforceAuth(clientId, clientSecret, userId, password, securityToken, oauthPath);
  }

  @Override
  protected void handleAuthResponse(OAuthAccessTokenResponse response)
      throws IOException {
    super.handleAuthResponse(response);
    try {
      instanceUrl = new URI(response.getParam(INSTANCE_URL));
    } catch (URISyntaxException e) {
      throw new IOException("Failed due to invalid instance url", e);
    }
  }
}
