package gobblin.util.jdbc;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;

import java.util.Properties;

import javax.sql.DataSource;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataSourceBuilder {
  private static final Logger LOG = LoggerFactory.getLogger(DataSourceBuilder.class);

  private String url;
  private String driver;
  private String userName;
  private String passWord;
  private Integer maxIdleConnections;
  private Integer maxActiveConnections;
  private String cryptoKeyLocation;
  private Boolean useStrongEncryption;
  private State state;

  public static DataSourceBuilder builder() {
    return new DataSourceBuilder();
  }

  public DataSourceBuilder url(String url) {
    this.url = url;
    return this;
  }

  public DataSourceBuilder driver(String driver) {
    this.driver = driver;
    return this;
  }

  public DataSourceBuilder userName(String userName) {
    this.userName = userName;
    return this;
  }

  public DataSourceBuilder passWord(String passWord) {
    this.passWord = passWord;
    return this;
  }

  public DataSourceBuilder maxIdleConnections(int maxIdleConnections) {
    this.maxIdleConnections = maxIdleConnections;
    return this;
  }

  public DataSourceBuilder maxActiveConnections(int maxActiveConnections) {
    this.maxActiveConnections = maxActiveConnections;
    return this;
  }

  public DataSourceBuilder cryptoKeyLocation(String cryptoKeyLocation) {
    this.cryptoKeyLocation = cryptoKeyLocation;
    return this;
  }

  public DataSourceBuilder useStrongEncryption(boolean useStrongEncryption) {
    this.useStrongEncryption = useStrongEncryption;
    return this;
  }

  public DataSourceBuilder state(State state) {
    this.state = state;
    return this;
  }

  public DataSource build() {
    validate();
    Properties properties = new Properties();
    if (state != null) {
      properties = state.getProperties();
    }
    properties.setProperty(DataSourceProvider.CONN_URL, url);
    properties.setProperty(DataSourceProvider.USERNAME, userName);
    properties.setProperty(DataSourceProvider.PASSWORD, passWord);
    properties.setProperty(DataSourceProvider.CONN_DRIVER, driver);
    if (!StringUtils.isEmpty(cryptoKeyLocation)) {
      properties.setProperty(ConfigurationKeys.ENCRYPT_KEY_LOC, cryptoKeyLocation);
    }

    if (maxIdleConnections != null) {
      properties.setProperty(DataSourceProvider.MAX_IDLE_CONNS, maxIdleConnections.toString());
    }

    if (maxActiveConnections != null) {
      properties.setProperty(DataSourceProvider.MAX_ACTIVE_CONNS, maxActiveConnections.toString());
    }

    if(useStrongEncryption != null) {
      properties.setProperty(ConfigurationKeys.ENCRYPT_USE_STRONG_ENCRYPTOR, useStrongEncryption.toString());
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Building DataSource with properties " + properties);
    }

    return new DataSourceProvider(properties).get();
  }

  private void validate() {
    validateNotEmpty(url, "url");
    validateNotEmpty(driver, "driver");
    validateNotEmpty(passWord, "passWord");
    validateTrue(maxIdleConnections == null || maxIdleConnections > 0, "maxIdleConnections should be a positive integer.");
    validateTrue(maxActiveConnections == null || maxActiveConnections > 0, "maxActiveConnections should be a positive integer.");
  }

  private void validateNotEmpty(String s, String name) {
    if(StringUtils.isEmpty(s)) {
      throw new IllegalArgumentException(name + " should not be empty.");
    }
  }

  private void validateTrue(boolean condition, String message) {
    if(!condition) {
      throw new IllegalArgumentException(message);
    }
  }

  @Override
  public String toString() {
    return "DataSourceBuilder [url=" + url + ", driver=" + driver + ", userName=" + userName + ", passWord=" + passWord
        + ", maxIdleConnections=" + maxIdleConnections + ", maxActiveConnections=" + maxActiveConnections
        + ", cryptoKeyLocation=" + cryptoKeyLocation + ", useStrongEncryption=" + useStrongEncryption + ", state="
        + state + "]";
  }
}
