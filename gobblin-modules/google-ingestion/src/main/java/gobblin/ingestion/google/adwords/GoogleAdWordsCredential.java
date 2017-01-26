package gobblin.ingestion.google.adwords;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Properties;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;

import com.google.api.ads.adwords.lib.client.AdWordsSession;
import com.google.api.ads.common.lib.auth.GoogleClientSecretsBuilder;
import com.google.api.ads.common.lib.exception.ValidationException;
import com.google.api.client.googleapis.auth.oauth2.GoogleAuthorizationCodeFlow;
import com.google.api.client.googleapis.auth.oauth2.GoogleAuthorizationCodeTokenRequest;
import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.auth.oauth2.GoogleTokenResponse;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.common.base.Charsets;
import com.google.common.collect.Lists;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.WorkUnitState;


public class GoogleAdWordsCredential {
  private static final String REDIRECT_URI = "urn:ietf:wg:oauth:2.0:oob";
  private static final ArrayList<String> SCOPES = Lists.newArrayList("https://www.googleapis.com/auth/adwords");
  private final String _clientId;
  private final String _clientSecret;
  private final String _appName;
  private final String _developerToken;
  private final String _refreshToken;

  /**
   * This main method is for getting an updated refresh token.
   * Configure the google_adwords_credential.properties file to do all necessary setup.
   */
  public static void main(String[] args)
      throws IOException, ValidationException {
    InputStream input = GoogleAdWordsCredential.class.getResourceAsStream("/google_adwords_credential.properties");
    Properties prop = new Properties();
    prop.load(input);

    GoogleAdWordsCredential credential =
        new GoogleAdWordsCredential(prop.getProperty(GoogleAdWordsSource.KEY_DEVELOPER_TOKEN),
            prop.getProperty(prop.getProperty(ConfigurationKeys.SOURCE_ENTITY)),
            prop.getProperty(GoogleAdWordsSource.KEY_CLIENT_ID),
            prop.getProperty(GoogleAdWordsSource.KEY_CLIENT_SECRET));
    String refreshToken = credential.getRefreshToken();
    System.out.println(String.format("The updated refresh token is: %s", refreshToken));
  }

  public GoogleAdWordsCredential(WorkUnitState state) {
    _developerToken = state.getProp(GoogleAdWordsSource.KEY_DEVELOPER_TOKEN);
    _refreshToken = state.getProp(GoogleAdWordsSource.KEY_REFRESH_TOKEN);

    _appName = state.getProp(ConfigurationKeys.SOURCE_ENTITY);
    _clientId = state.getProp(GoogleAdWordsSource.KEY_CLIENT_ID);
    _clientSecret = state.getProp(GoogleAdWordsSource.KEY_CLIENT_SECRET);
  }

  /**
   * This constructor is for updating the refresh token only
   */
  private GoogleAdWordsCredential(String developerToken, String appName, String clientId, String clientSecret) {
    _developerToken = developerToken;
    _refreshToken = null;

    _appName = appName;
    _clientId = clientId;
    _clientSecret = clientSecret;
  }

  public AdWordsSession.ImmutableAdWordsSession buildRootSession()
      throws ValidationException {
    GoogleCredential credential = buildGoogleCredential();
    credential.setRefreshToken(_refreshToken);
    return new AdWordsSession.Builder().withDeveloperToken(_developerToken).withOAuth2Credential(credential)
        .buildImmutable();
  }

  private String getRefreshToken()
      throws ValidationException, IOException {
    GoogleClientSecrets clientSecrets = getGoogleClientSecrets();
    GoogleAuthorizationCodeFlow authorizationFlow =
        new GoogleAuthorizationCodeFlow.Builder(new NetHttpTransport(), new JacksonFactory(), clientSecrets, SCOPES)
            // Set the access type to offline so that the token can be refreshed.
            // By default, the library will automatically refresh tokens when it can
            // But this can be turned off by setting api.adwords.refreshOAuth2Token=false.
            .setAccessType("offline").build();

//    GoogleCredential gc = GoogleCredential.fromStream(new FileInputStream(""))
//        .createScoped(Collections.singletonList("SCOPE"));
//    AdWordsSession build = new AdWordsSession.Builder().fromFile().build();

    String authorizeUrl = authorizationFlow.newAuthorizationUrl().setRedirectUri(REDIRECT_URI).build();
    System.out.println(String.format("Paste this url in your browser:%n%s%n", authorizeUrl));
    System.out.println("Type the code you received here: ");
    String authorizationCode = new BufferedReader(new InputStreamReader(System.in, Charsets.UTF_8)).readLine();

    // Authorize the OAuth2 token.
    GoogleAuthorizationCodeTokenRequest tokenRequest = authorizationFlow.newTokenRequest(authorizationCode);
    tokenRequest.setRedirectUri(REDIRECT_URI);
    GoogleTokenResponse tokenResponse = tokenRequest.execute();

    GoogleCredential credential = buildGoogleCredential(clientSecrets);

    // Set authorized credentials.
    credential.setFromTokenResponse(tokenResponse);
    return credential.getRefreshToken();
  }

  private GoogleCredential buildGoogleCredential()
      throws ValidationException {
    return buildGoogleCredential(getGoogleClientSecrets());
  }

  private static GoogleCredential buildGoogleCredential(GoogleClientSecrets clientSecrets) {
    return new GoogleCredential.Builder().setTransport(new NetHttpTransport()).setJsonFactory(new JacksonFactory())
        .setClientSecrets(clientSecrets).build();
  }

  private GoogleClientSecrets getGoogleClientSecrets()
      throws ValidationException {
    return getGoogleClientSecrets(_clientId, _clientSecret, _appName);
  }

  private static GoogleClientSecrets getGoogleClientSecrets(String clientId, String clientSecrete, String appName)
      throws ValidationException {
    Configuration config = new PropertiesConfiguration();
    config.setProperty("api.adwords.clientId", clientId);
    config.setProperty("api.adwords.clientSecret", clientSecrete);
    //Optional. Set a friendly application name identifier.
    config.setProperty("api.adwords.userAgent", appName);
    config.setProperty("api.adwords.isPartialFailure", false);
    //disable automatic OAuth2 token refreshing. Default is enabled.
    config.setProperty("api.adwords.refreshOAuth2Token", false);

    /* Other available settings

# Change the AdWords API endpoint. Optional.
# api.adwords.endpoint=https://adwords.google.com/

# [JVM] The following properties are JVM-level properties and
# are read and set only ONCE, when the AdWordsServices
# class is first loaded.

# Enable/disable compression. Default is disabled. See the following link for
# more information:
# https://github.com/googleads/googleads-java-lib#user-content-how-do-i-enable-compression
# api.adwords.useCompression=false

# Default report download connect/read timeout. Defaults to 3 minutes if omitted.
# Can be overridden on each instance of GoogleAdWordsReportDownloader via
# GoogleAdWordsReportDownloader.setReportDownloadTimeout(timeoutInMillis).
# Specify value in milliseconds.
# api.adwords.reportDownloadTimeout=180000


# Set the AdWords API request timeout in milliseconds. Defaults to 1200000.
# api.adwords.soapRequestTimeout=1200000

# Optional. Set to false to not include utility usage information in the user agent in requests.
# Defaults to true (usage included).
# api.adwords.includeUtilitiesInUserAgent=true

     */

    return new GoogleClientSecretsBuilder().forApi(GoogleClientSecretsBuilder.Api.ADWORDS).from(config).build();
  }
}
