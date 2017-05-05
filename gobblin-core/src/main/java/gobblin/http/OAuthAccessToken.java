package gobblin.http;

import java.io.IOException;

import org.apache.oltu.oauth2.client.OAuthClient;
import org.apache.oltu.oauth2.client.URLConnectionClient;
import org.apache.oltu.oauth2.client.request.OAuthClientRequest;
import org.apache.oltu.oauth2.client.response.OAuthAccessTokenResponse;
import org.apache.oltu.oauth2.common.OAuth;
import org.apache.oltu.oauth2.common.exception.OAuthProblemException;
import org.apache.oltu.oauth2.common.exception.OAuthSystemException;
import org.apache.oltu.oauth2.common.message.types.GrantType;


import lombok.Getter;
import lombok.extern.log4j.Log4j;


@Log4j
public class OAuthAccessToken {
  private final String clientId;
  private final String clientSecret;
  private final String userId;
  private final String password;
  private final String securityToken;
  private final String oauthPath;

  @Getter
  protected String accessToken;

  public OAuthAccessToken(String clientId, String clientSecret, String userId, String password, String securityToken,
      String oauthPath) {
    this.clientId = clientId;
    this.clientSecret = clientSecret;
    this.userId = userId;
    this.password = password;
    this.securityToken = securityToken;
    this.oauthPath = oauthPath;
  }

  protected OAuthClientRequest buildAuthRequest() {
    OAuthClientRequest request;
    try {
      request = OAuthClientRequest.tokenLocation(oauthPath)
          .setGrantType(GrantType.PASSWORD)
          .setClientId(clientId)
          .setClientSecret(clientSecret)
          .setUsername(userId)
          .setPassword(password + securityToken).buildQueryMessage();
    } catch (OAuthSystemException e) {
      throw new RuntimeException("Failed to construct auth request", e);
    }
    return request;
  }

  public boolean authenticate() {
    log.info("Getting Oauth2 access token.");
    OAuthClient client = new OAuthClient(new URLConnectionClient());
    OAuthClientRequest request = buildAuthRequest();
    try {
      OAuthAccessTokenResponse response = client.accessToken(request, OAuth.HttpMethod.POST);
      handleAuthResponse(response);
      return true;
    } catch (OAuthSystemException | OAuthProblemException | IOException e) {
      throw new RuntimeException("Failed getting access token", e);
    }
  }

  protected void handleAuthResponse(OAuthAccessTokenResponse response) throws IOException {
    accessToken = response.getAccessToken();
  }
}
