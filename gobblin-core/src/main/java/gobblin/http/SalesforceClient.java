package gobblin.http;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.impl.client.HttpClientBuilder;
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
import com.typesafe.config.ConfigValueFactory;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.log4j.Log4j;

import gobblin.http.ApacheHttpClient;
import gobblin.http.SalesforceAuth;


/**
 * An {@link ApacheHttpClient} to communicate with Salesforce services
 */
@Log4j
public class SalesforceClient extends ApacheHttpClient {
  private final SalesforceAuth auth;

  @Getter @Setter
  private String accessToken;

  public SalesforceClient(SalesforceAuth auth, HttpClientBuilder builder, Config config) {
    super(builder, config);
    this.auth = auth;
    authenticate();
  }

  public void authenticate() {
    log.info("Authenticating");
    accessToken = null;
    serverHost = null;
    if (auth.authenticate()){
      accessToken = auth.getAccessToken();
      serverHost = auth.getInstanceUrl();
    }
  }

  @Override
  public boolean connect(URI uri) throws IOException {
    if (!StringUtils.isEmpty(accessToken)) {
      return true;
    }
    authenticate();
    return true;
  }

  @Override
  public CloseableHttpResponse sendRequest(HttpUriRequest request)
      throws IOException {
    if (Strings.isNullOrEmpty(accessToken)) {
      authenticate();
    }
    request.addHeader(HttpHeaders.AUTHORIZATION, "OAuth " + accessToken);
    return super.sendRequest(request);
  }
}
