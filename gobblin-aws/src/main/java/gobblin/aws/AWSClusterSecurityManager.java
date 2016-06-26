package gobblin.aws;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClient;
import com.amazonaws.services.securitytoken.model.AssumeRoleRequest;
import com.amazonaws.services.securitytoken.model.AssumeRoleResult;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.AbstractIdleService;
import com.typesafe.config.Config;

import gobblin.util.ExecutorsUtils;

/**
 * A class for managing AWS login and credentials renewal
 *
 * @author Abhishek Tiwari
 */
public class AWSClusterSecurityManager extends AbstractIdleService {

  private static final Logger LOGGER = LoggerFactory.getLogger(AWSClusterSecurityManager.class);

  private final Config config;

  private String serviceAccessKey;
  private String serviceSecretKey;
  private boolean clientAssumeRole;
  private String clientRoleArn;
  private String clientExternalId;
  private String clientSessionId;

  private BasicAWSCredentials basicAWSCredentials;
  private BasicSessionCredentials basicSessionCredentials;

  private final long refreshIntervalInMinutes;

  private final ScheduledExecutorService loginExecutor;

  public AWSClusterSecurityManager(Config config) {
    this.config = config;

    this.refreshIntervalInMinutes = config.getLong(GobblinAWSConfigurationKeys.CREDENTIALS_REFRESH_INTERVAL_IN_MINUTES);

    this.loginExecutor = Executors.newSingleThreadScheduledExecutor(
        ExecutorsUtils.newThreadFactory(Optional.of(LOGGER), Optional.of("LoginExecutor")));
  }

  private void fetchLoginConfiguration() {
    this.serviceAccessKey = config.getString(GobblinAWSConfigurationKeys.SERVICE_ACCESS_KEY);
    this.serviceSecretKey = config.getString(GobblinAWSConfigurationKeys.SERVICE_SECRET_KEY);
    this.clientAssumeRole = config.getBoolean(GobblinAWSConfigurationKeys.CLIENT_ASSUME_ROLE_KEY);

    // If we are running on behalf of another AWS user, we need to fetch temporary credentials for a
    // .. configured role
    if (this.clientAssumeRole) {
      this.clientRoleArn = config.getString(GobblinAWSConfigurationKeys.CLIENT_ROLE_ARN_KEY);
      this.clientExternalId = config.getString(GobblinAWSConfigurationKeys.CLIENT_EXTERNAL_ID_KEY);
      this.clientSessionId = config.getString(GobblinAWSConfigurationKeys.CLIENT_SESSION_ID_KEY);
    }
  }

  @Override
  protected void startUp()
      throws Exception {
    LOGGER.info("Starting the " + AWSClusterSecurityManager.class.getSimpleName());

    LOGGER.info(
        String.format("Scheduling the credentials refresh task with an interval of %d minute(s)",
            this.refreshIntervalInMinutes));

    // Schedule the login task
    this.loginExecutor.scheduleAtFixedRate(new Runnable() {
      @Override
      public void run() {
        try {
          login();
        } catch (IOException ioe) {
          LOGGER.error("Failed to login", ioe);
          throw Throwables.propagate(ioe);
        }
      }
    }, 0, this.refreshIntervalInMinutes, TimeUnit.MINUTES);
  }

  @Override
  protected void shutDown()
      throws Exception {

  }

  private void login() throws IOException {
    // Refresh login configuration details from config
    fetchLoginConfiguration();

    // Primary AWS user login
    this.basicAWSCredentials = new BasicAWSCredentials(this.serviceAccessKey, this.serviceSecretKey);

    // If running on behalf of another AWS user,
    // .. assume role as configured
    if (this.clientAssumeRole) {
      AssumeRoleRequest assumeRoleRequest = new AssumeRoleRequest()
          .withRoleSessionName(this.clientSessionId)
          .withExternalId(this.clientExternalId)
          .withRoleArn(this.clientRoleArn);

      AWSSecurityTokenServiceClient stsClient = new AWSSecurityTokenServiceClient(this.basicAWSCredentials);

      AssumeRoleResult assumeRoleResult = stsClient.assumeRole(assumeRoleRequest);

      this.basicSessionCredentials = new BasicSessionCredentials(
          assumeRoleResult.getCredentials().getAccessKeyId(),
          assumeRoleResult.getCredentials().getSecretAccessKey(),
          assumeRoleResult.getCredentials().getSessionToken()
      );
    }
  }

  public boolean isAssumeRoleEnabled() {
    return this.clientAssumeRole;
  }

  public BasicAWSCredentials getBasicAWSCredentials() {
    return this.basicAWSCredentials;
  }

  public BasicSessionCredentials getBasicSessionCredentials() {
    if (!this.clientAssumeRole) {
      throw new IllegalStateException("AWS Security manager is not configured to run on behalf of another AWS user. "
          + "Use getBasicAWSCredentials() instead");
    }
    return this.basicSessionCredentials;
  }
}
