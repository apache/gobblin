package gobblin.ingestion.google.adwords;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gobblin.configuration.WorkUnitState;
import gobblin.source.extractor.Extractor;
import gobblin.source.extractor.extract.QueryBasedSource;


public class GoogleAdWordsSource extends QueryBasedSource<String, String[]> {
  /**
   * Must provide.
   * Specify the master customer id. Standard format that has dashes between numbers is acceptable.
   */
  public static final String KEY_MASTER_CUSTOMER = "source.google_adwords.master_customer_id";
  /**
   * Must provide.
   * Specify the report name you want to download.
   * The reports configured here must be convertible to ReportDefinitionReportType enums
   */
  public static final String KEY_REPORT = "source.google_adwords.request.report_name";
  /**
   * Must provide.
   * Specify the date range type you want to download.
   * The date range configured here must be convertible to ReportDefinitionDateRangeType enums
   */
  public static final String KEY_DATE_RANGE = "source.google_adwords.request.date_range_type";
  /**
   * Optional, default to false.
   * Only used when date_range_type is set to "custom_date".
   * Determine whether to partition the custom date range into each single day while downloading reports.
   * This is necessary because some reports only support daily download, for example, CLICK_PERFORMANCE.
   */
  public static final String KEY_CUSTOM_DATE_DAILY = "source.google_adwords.request.custom_date.daily";
  /**
   * Optional.
   * Specify the column names for the report you want to download. Separate column names by commas
   * If omitted, the whole report will be downloaded.
   */
  public static final String KEY_COLUMN_NAMES = "source.google_adwords.request.column_names";
  /**
   * Optional.
   * Specify the exact list of accounts you want to download the reports. Separate accounts by commas
   * If omitted, the all accounts with that report will be downloaded.
   * Can overwrite KEY_ACCOUNTS_EXCLUDE if both are configured.
   */
  public static final String KEY_ACCOUNTS_EXACT = "source.google_adwords.request.accounts.exact";
  /**
   * Optional.
   * Specify the list of accounts you want to exclude from all available accounts. Separate accounts by commas
   * Will be overwritten by KEY_ACCOUNTS_EXACT if both are configured.
   */
  public static final String KEY_ACCOUNTS_EXCLUDE = "source.google_adwords.request.accounts.exclude";
  /**
   * Must provide.
   * Specify the developer token.
   * This can be found in your Account Setting => AdWords API Center
   */
  public static final String KEY_DEVELOPER_TOKEN = "source.google_adwords.credential.developer_token";
  /**
   * Must provide.
   * Specify the refresh token.
   * This can be got by running the main method of GoogleAdWordsCredential.
   */
  public static final String KEY_REFRESH_TOKEN = "source.google_adwords.credential.refresh_token";
  /**
   * Must provide when getting the refresh token.
   * Give a friendly application name.
   */
  public static final String KEY_APPLICATION_NAME = "source.google_adwords.app_name";
  /**
   * Must provide when getting the refresh token.
   * Specify the client id of your OAuth 2.0 credential. This can be found in the Google API Manger.
   */
  public static final String KEY_CLIENT_ID = "source.google_adwords.credential.client_id";
  /**
   * Must provide when getting the refresh token.
   * Specify the client secret of your OAuth 2.0 credential. This can be found in the Google API Manger.
   */
  public static final String KEY_CLIENT_SECRET = "source.google_adwords.credential.client_secret";
  /**
   * Optional.
   * Specify the type of job you want to run. Set this to true if you want to get an updated refresh token.
   * If omitted, the service will download the reports.
   */
  public static final String KEY_GET_REFRESH_TOKEN = "source.google_adwords.get_refresh_token";
  /**
   * Optional. Default to true.
   */
  public static final String KEY_REPORTING_SKIP_REPORT_HEADER =
      "source.google_adwords.request.reporting.skip_report_header";
  /**
   * Optional. Default to true.
   */
  public static final String KEY_REPORTING_SKIP_COLUMN_HEADER =
      "source.google_adwords.request.reporting.skip_column_header";
  /**
   * Optional. Default to true.
   */
  public static final String KEY_REPORTING_SKIP_REPORT_SUMMARY =
      "source.google_adwords.request.reporting.skip_report_summary";
  /**
   * Optional. Default to false.
   */
  public static final String KEY_REPORTING_INCLUDE_ZERO_IMPRESSION =
      "source.google_adwords.request.reporting.include_zero_impressions";
  /**
   * Optional. Default to false.
   */
  public static final String KEY_REPORTING_USE_RAW_ENUM_VALUES =
      "source.google_adwords.request.reporting.use_raw_enum_values";

  private final static Logger LOG = LoggerFactory.getLogger(GoogleAdWordsSource.class);

  @Override
  public Extractor<String, String[]> getExtractor(WorkUnitState state)
      throws IOException {
    try {
      return new GoogleAdWordsExtractor(state);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }
}
