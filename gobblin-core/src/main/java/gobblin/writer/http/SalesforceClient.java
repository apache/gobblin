package gobblin.writer.http;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.oltu.oauth2.client.OAuthClient;
import org.apache.oltu.oauth2.client.URLConnectionClient;
import org.apache.oltu.oauth2.client.request.OAuthClientRequest;
import org.apache.oltu.oauth2.client.response.OAuthJSONAccessTokenResponse;
import org.apache.oltu.oauth2.common.OAuth;
import org.apache.oltu.oauth2.common.exception.OAuthProblemException;
import org.apache.oltu.oauth2.common.exception.OAuthSystemException;
import org.apache.oltu.oauth2.common.message.types.GrantType;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.log4j.Log4j;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.http.ApacheHttpClient;
import gobblin.password.PasswordManager;
import gobblin.writer.exception.NonTransientException;


/**
 * An {@link ApacheHttpClient} to communicate with Salesforce services
 */
@Log4j
public class SalesforceClient extends ApacheHttpClient {
  static final String SFDC_PREFIX = SalesforceWriterBuilder.SFDC_PREFIX;
  static final String CLIENT_ID = SFDC_PREFIX + "client_id";
  static final String CLIENT_SECRET = SFDC_PREFIX + "client_secret";
  static final String USER_ID = SFDC_PREFIX + "user_id";
  static final String PASSWORD = SFDC_PREFIX + "password";
  static final String SFDC_ENCRYPT_KEY_LOC = SFDC_PREFIX + ConfigurationKeys.ENCRYPT_KEY_LOC;
  static final String USE_STRONG_ENCRYPTION = SFDC_PREFIX + "strong_encryption";
  static final String SECURITY_TOKEN = SFDC_PREFIX + "security_token";

  private static final Config FALLBACK = ConfigFactory.parseMap(
      ImmutableMap.<String, String>builder()
          .put(ApacheHttpClient.STATIC_SVC_ENDPOINT, "https://login.salesforce.com/services/oauth2/token")
          .put(SECURITY_TOKEN, "")
          .build()
  );

  private String clientId;
  private String clientSecret;
  private String userId;
  private String password;
  private String securityToken;
  private final URI oauthEndpoint;

  @Getter @Setter
  private String accessToken;

  public SalesforceClient(State state, Config config) {
    super(state, config);

    config = config.withFallback(FALLBACK);
    clientId = config.getString(CLIENT_ID);
    clientSecret = config.getString(CLIENT_SECRET);
    userId = config.getString(USER_ID);
    password = config.getString(PASSWORD);
    securityToken = config.getString(SECURITY_TOKEN);

    if (config.hasPath(SFDC_ENCRYPT_KEY_LOC)) {
      Properties props = new Properties();
      if (config.hasPath(USE_STRONG_ENCRYPTION)) {
        props.put(ConfigurationKeys.ENCRYPT_USE_STRONG_ENCRYPTOR, config.getString(USE_STRONG_ENCRYPTION));
      }

      props.put(ConfigurationKeys.ENCRYPT_KEY_LOC, config.getString(SFDC_ENCRYPT_KEY_LOC));
      password = PasswordManager.getInstance(props).readPassword(password);
    }
    oauthEndpoint = serverHost;

    try {
      connect(oauthEndpoint);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean connect(URI uri) throws IOException {
    if (!StringUtils.isEmpty(accessToken)) {
      return true;
    }

    try {
      log.info("Getting Oauth2 access token.");
      OAuthClientRequest request = OAuthClientRequest.tokenLocation(uri.toString())
          .setGrantType(GrantType.PASSWORD)
          .setClientId(clientId)
          .setClientSecret(clientSecret)
          .setUsername(userId)
          .setPassword(password + securityToken).buildQueryMessage();
      OAuthClient client = new OAuthClient(new URLConnectionClient());
      OAuthJSONAccessTokenResponse response = client.accessToken(request, OAuth.HttpMethod.POST);

      accessToken = response.getAccessToken();
      // Update serverHost to be the endpoint for fetching data
      serverHost = new URI(response.getParam("instance_url"));
    } catch (OAuthProblemException e) {
      throw new NonTransientException("Error while authenticating with Oauth2", e);
    } catch (OAuthSystemException e) {
      throw new RuntimeException("Failed getting access token", e);
    } catch (URISyntaxException e) {
      throw new RuntimeException("Failed due to invalid instance url", e);
    }

    return true;
  }

  @Override
  public CloseableHttpResponse sendRequest(HttpUriRequest request)
      throws IOException {
    if (Strings.isNullOrEmpty(accessToken)) {
      log.info("Reacquiring access token.");
      connect(oauthEndpoint);
    }
    request.addHeader(HttpHeaders.AUTHORIZATION, "OAuth " + accessToken);
    return super.sendRequest(request);
  }
}
