package gobblin.ingestion.google.adwords;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gobblin.configuration.WorkUnitState;
import gobblin.source.extractor.Extractor;
import gobblin.source.extractor.extract.QueryBasedSource;


public class GoogleAdWordsSource extends QueryBasedSource<String, String[]> {
  private static final String ADWORDS = "source.google_adwords.";
  /**
   * Must provide.
   * Specify the master customer id. Standard format that has dashes between numbers is acceptable.
   */
  static final String KEY_MASTER_CUSTOMER = ADWORDS + "master_customer_id";
  /**
   * Optional.
   * Specify the type of job you want to run. Set this to true if you want to get an updated refresh token.
   * If omitted, the service will download the reports.
   */
  public static final String KEY_GET_REFRESH_TOKEN = ADWORDS + "get_refresh_token";
  // ================================================
  // ========= REQUEST CONFIGURATION BEGIN ==========
  // ================================================
  private static final String REQUEST = ADWORDS + "request.";
  /**
   * Must provide.
   * Specify the report name you want to download.
   * The reports configured here must be convertible to ReportDefinitionReportType enums
   */
  static final String KEY_REPORT = REQUEST + "report_name";
  /**
   * Must provide.
   * Specify the date range type you want to download.
   * The date range configured here must be convertible to ReportDefinitionDateRangeType enums
   */
  static final String KEY_DATE_RANGE = REQUEST + "date_range_type";
  /**
   * Optional, default to false.
   * Only used when date_range_type is set to "custom_date".
   * Determine whether to partition the custom date range into each single day while downloading reports.
   * This is necessary because some reports only support daily download, for example, CLICK_PERFORMANCE.
   */
  static final String KEY_CUSTOM_DATE_DAILY = REQUEST + "custom_date.daily";
  /**
   * Optional, default to whole report, all columns
   * Specify the column names for the report you want to download. Separate column names by commas
   */
  static final String KEY_COLUMN_NAMES = REQUEST + "column_names";
  /**
   * Optional, default to 8.
   * Configure the number of threads that will be used for downloading reports.
   */
  static final String KEY_THREADS = REQUEST + "threads";
  /**
   * Optional.
   * Specify the exact list of accounts you want to download the reports. Separate accounts by commas
   * If omitted, the all accounts with that report will be downloaded.
   * Can overwrite KEY_ACCOUNTS_EXCLUDE if both are configured.
   */
  static final String KEY_ACCOUNTS_EXACT = REQUEST + "accounts.exact";
  /**
   * Optional.
   * Specify the list of accounts you want to exclude from all available accounts. Separate accounts by commas
   * Will be overwritten by KEY_ACCOUNTS_EXACT if both are configured.
   */
  static final String KEY_ACCOUNTS_EXCLUDE = REQUEST + "accounts.exclude";
  // ==================================================
  // ========= REPORTING CONFIGURATION BEGIN ==========
  // ==================================================
  private static final String REQUEST_REPORTING = REQUEST + "reporting.";
  /**
   * Optional. Default to true.
   */
  static final String KEY_REPORTING_SKIP_REPORT_HEADER = REQUEST_REPORTING + "skip_report_header";
  /**
   * Optional. Default to true.
   */
  static final String KEY_REPORTING_SKIP_COLUMN_HEADER = REQUEST_REPORTING + "skip_column_header";
  /**
   * Optional. Default to true.
   */
  static final String KEY_REPORTING_SKIP_REPORT_SUMMARY = REQUEST_REPORTING + "skip_report_summary";
  /**
   * Optional. Default to false.
   */
  static final String KEY_REPORTING_INCLUDE_ZERO_IMPRESSION = REQUEST_REPORTING + "include_zero_impressions";
  /**
   * Optional. Default to false.
   */
  static final String KEY_REPORTING_USE_RAW_ENUM_VALUES = REQUEST_REPORTING + "use_raw_enum_values";
  // ==================================================
  // ========= REPORTING CONFIGURATION BEGIN ==========
  // ==================================================

  // ==============================================
  // ========= REQUEST CONFIGURATION END ==========
  // ==============================================

  // ===================================================
  // ========= CREDENTIAL CONFIGURATION BEGIN ==========
  // ===================================================
  private static final String CREDENTIAL = ADWORDS + "credential.";
  /**
   * Must provide.
   * Specify the developer token.
   * This can be found in your Account Setting => AdWords API Center
   */
  static final String KEY_DEVELOPER_TOKEN = CREDENTIAL + "developer_token";
  /**
   * Must provide.
   * Specify the refresh token.
   * This can be got by running the main method of GoogleAdWordsCredential.
   */
  static final String KEY_REFRESH_TOKEN = CREDENTIAL + "refresh_token";
  /**
   * Must provide when getting the refresh token.
   * Specify the client id of your OAuth 2.0 credential. This can be found in the Google API Manger.
   */
  static final String KEY_CLIENT_ID = CREDENTIAL + "client_id";
  /**
   * Must provide when getting the refresh token.
   * Specify the client secret of your OAuth 2.0 credential. This can be found in the Google API Manger.
   */
  static final String KEY_CLIENT_SECRET = CREDENTIAL + "client_secret";
  // =================================================
  // ========= CREDENTIAL CONFIGURATION END ==========
  // =================================================

  // ================================
  // ========= DEBUG BEGIN ==========
  // ================================
  private static final String DEBUG = ADWORDS + "debug.";
  /**
   * For Debugging purpose only.
   * Specify the output path for unzipped strings. The string here is the whole unzipped string before split to rows.
   */
  static final String KEY_DEBUG_PATH_STRING = DEBUG + "string_output_path";
  /**
   * For Debugging purpose only.
   * Specify the output path for unzipped files.
   * If configured, Gobblin will not output any data because the downloaded stream is closed and can only be read once.
   */
  static final String KEY_DEBUG_PATH_FILE = DEBUG + "file_output_path";
  // ================================
  // ========= DEBUG BEGIN ==========
  // ================================
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
