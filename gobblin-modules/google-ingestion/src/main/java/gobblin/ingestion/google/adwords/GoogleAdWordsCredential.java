package gobblin.ingestion.google.adwords;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  public static final String REDIRECT_URI = "urn:ietf:wg:oauth:2.0:oob";
  public static final ArrayList<String> SCOPES = Lists.newArrayList("https://www.googleapis.com/auth/adwords");
  private final static Logger LOG = LoggerFactory.getLogger(GoogleAdWordsCredential.class);
  public final String _clientId;
  public final String _clientSecret;
  public final String _appName;
  private final String _developerToken;
  private final String _refreshToken;

  public GoogleAdWordsCredential(WorkUnitState state) {
    _developerToken = state.getProp(GoogleAdWordsSource.KEY_DEVELOPER_TOKEN);
    _refreshToken = state.getProp(GoogleAdWordsSource.KEY_REFRESH_TOKEN);

    //For getting refresh tokens.
    _appName = state.getProp(ConfigurationKeys.SOURCE_ENTITY);
    _clientId = state.getProp(GoogleAdWordsSource.KEY_CLIENT_ID);
    _clientSecret = state.getProp(GoogleAdWordsSource.KEY_CLIENT_SECRET);
  }

  public AdWordsSession.ImmutableAdWordsSession buildRootSession()
      throws ValidationException {
    GoogleCredential credential = buildGoogleCredential();
    credential.setRefreshToken(_refreshToken);
    return new AdWordsSession.Builder().withDeveloperToken(_developerToken).withOAuth2Credential(credential)
        .buildImmutable();
  }

  public String getRefreshToken()
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
    LOG.info(String.format("Paste this url in your browser:%n%s%n", authorizeUrl));
    LOG.info("Type the code you received here: ");
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

  public GoogleCredential buildGoogleCredential()
      throws ValidationException {
    return buildGoogleCredential(getGoogleClientSecrets());
  }

  public static GoogleCredential buildGoogleCredential(GoogleClientSecrets clientSecrets) {
    return new GoogleCredential.Builder().setTransport(new NetHttpTransport()).setJsonFactory(new JacksonFactory())
        .setClientSecrets(clientSecrets).build();
  }

  public GoogleClientSecrets getGoogleClientSecrets()
      throws ValidationException {
    return getGoogleClientSecrets(_clientId, _clientSecret, _appName);
  }

  public static GoogleClientSecrets getGoogleClientSecrets(String clientId, String clientSecrete, String appName)
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
