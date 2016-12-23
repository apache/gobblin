package gobblin.ingestion.google.adwords;

import java.io.IOException;
import java.rmi.RemoteException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.ads.adwords.axis.factory.AdWordsServices;
import com.google.api.ads.adwords.axis.v201609.cm.ReportDefinitionField;
import com.google.api.ads.adwords.axis.v201609.cm.ReportDefinitionServiceInterface;
import com.google.api.ads.adwords.axis.v201609.mcm.ManagedCustomer;
import com.google.api.ads.adwords.lib.client.AdWordsSession;
import com.google.api.ads.adwords.lib.jaxb.v201609.ReportDefinitionDateRangeType;
import com.google.api.ads.adwords.lib.jaxb.v201609.ReportDefinitionReportType;
import com.google.api.ads.common.lib.exception.ValidationException;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Sets;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import avro.shaded.com.google.common.collect.Lists;

import gobblin.configuration.WorkUnitState;
import gobblin.converter.avro.JsonElementConversionFactory;
import gobblin.source.extractor.DataRecordException;
import gobblin.source.extractor.Extractor;


public class GoogleAdWordsExtractor implements Extractor<String, String[]> {

  private final static Logger LOG = LoggerFactory.getLogger(GoogleAdWordsExtractor.class);
  private final static Splitter splitter = Splitter.on(",").omitEmptyStrings().trimResults();
  private WorkUnitState _state;
  GoogleAdWordsExtractorIterator _iterator;
  private String _schema;

  private final static HashMap<String, JsonElementConversionFactory.Type> typeConversionMap = new HashMap<>();

  static {
    typeConversionMap.put("string", JsonElementConversionFactory.Type.STRING);
    typeConversionMap.put("integer", JsonElementConversionFactory.Type.INT);
    typeConversionMap.put("long", JsonElementConversionFactory.Type.LONG);
    typeConversionMap.put("float", JsonElementConversionFactory.Type.FLOAT);
    typeConversionMap.put("double", JsonElementConversionFactory.Type.DOUBLE);
    typeConversionMap.put("boolean", JsonElementConversionFactory.Type.BOOLEAN);
  }

  public GoogleAdWordsExtractor(WorkUnitState state)
      throws Exception {
    _state = state;
    GoogleAdWordsCredential credential = new GoogleAdWordsCredential(state);
    AdWordsSession.ImmutableAdWordsSession rootSession = credential.buildRootSession();

    ReportDefinitionReportType reportType =
        ReportDefinitionReportType.valueOf(state.getProp(GoogleAdWordsSource.KEY_REPORT).toUpperCase() + "_REPORT");
    ReportDefinitionDateRangeType dateRangeType =
        ReportDefinitionDateRangeType.valueOf(state.getProp(GoogleAdWordsSource.KEY_DATE_RANGE).toUpperCase());
    if (!dateRangeType.equals(ReportDefinitionDateRangeType.CUSTOM_DATE) && !dateRangeType
        .equals(ReportDefinitionDateRangeType.ALL_TIME)) {
      throw new UnsupportedOperationException("Only support date range of custom_date or all_time");
    }

    String columnNamesString = state.getProp(GoogleAdWordsSource.KEY_COLUMN_NAMES, "");
    List<String> columnNames =
        columnNamesString.trim().isEmpty() ? null : Lists.newArrayList(splitter.split(columnNamesString));

    HashMap<String, String> allFields = getReportFields(rootSession, reportType);
    _schema = getDownloadFields(allFields, reportType, columnNames);
    LOG.info(String.format("The schema for report %s is: %s", reportType, _schema));

    _iterator = new GoogleAdWordsExtractorIterator(
        new GoogleAdWordsReportDownloader(rootSession, _state, reportType, dateRangeType, _schema),
        getConfiguredAccounts(rootSession, state));
  }

  /**
   * 1. Get all available non-manager accounts.
   * 2. If exactAccounts are provided, validate that all exactAccounts are a subset of step 1.
   * 3. If exclusiveAccounts are provided, remove them from step 1.
   */
  static Collection<String> getConfiguredAccounts(AdWordsSession rootSession, WorkUnitState state)
      throws ValidationException, RemoteException {

    String masterCustomerId = state.getProp(GoogleAdWordsSource.KEY_MASTER_CUSTOMER);
    String exactAccountString = state.getProp(GoogleAdWordsSource.KEY_ACCOUNTS_EXACT, "");
    Set<String> exactAccounts =
        exactAccountString.trim().isEmpty() ? null : Sets.newHashSet(splitter.split(exactAccountString));
    String exclusiveAccountString = state.getProp(GoogleAdWordsSource.KEY_ACCOUNTS_EXCLUDE, "");
    Set<String> exclusiveAccounts =
        exclusiveAccountString.trim().isEmpty() ? null : Sets.newHashSet(splitter.split(exclusiveAccountString));

    GoogleAdWordsAccountManager accountManager = new GoogleAdWordsAccountManager(rootSession);
    Map<Long, ManagedCustomer> availableAccounts = accountManager.getChildrenAccounts(masterCustomerId, false);
    Set<String> available = new HashSet<>();
    for (Map.Entry<Long, ManagedCustomer> account : availableAccounts.entrySet()) {
      available.add(Long.toString(account.getKey()));
    }
    LOG.info(
        String.format("Found %d available accounts for your master account %s", available.size(), masterCustomerId));

    if (exactAccounts != null) {
      Sets.SetView<String> difference = Sets.difference(exactAccounts, available);
      if (difference.isEmpty()) {
        return exactAccounts;
      } else {
        String msg = String
            .format("The following accounts configured in the exact list don't exist under master account %s: %s",
                masterCustomerId, Joiner.on(",").join(difference));
        LOG.error(msg);
        throw new RuntimeException(msg);
      }
    }

    if (exclusiveAccounts != null && !exclusiveAccounts.isEmpty()) {
      available.removeAll(exclusiveAccounts);
    }
    return available;
  }

  @Override
  public String getSchema()
      throws IOException {
    return _schema;
  }

  @Override
  public String[] readRecord(@Deprecated String[] reuse)
      throws DataRecordException, IOException {
    while (_iterator.hasNext()) {
      return _iterator.next();
    }
    return null;
  }

  @Override
  public long getExpectedRecordCount() {
    return 0;
  }

  @Override
  public long getHighWatermark() {
    throw new UnsupportedOperationException("This method has been deprecated!");
  }

  @Override
  public void close()
      throws IOException {
//    if (_successful) {
//      LOG.info(String.format("Successfully finished fetching data from Google Search Console from %s to %s.",
//          dateFormatter.print(_startDate), dateFormatter.print(_endDate)));
//      _wuState.setActualHighWatermark(
//          new LongWatermark(Long.parseLong(watermarkFormatter.print(_endDate.plusDays(1)))));
//    } else {
//      LOG.warn(String.format("Had problems fetching all data from Google Search Console from %s to %s.",
//          dateFormatter.print(_startDate), dateFormatter.print(_endDate)));
//    }
  }

  static String getDownloadFields(HashMap<String, String> allFields, ReportDefinitionReportType reportType,
      List<String> requestedColumns) {
    JsonArray schema = new JsonArray();
    HashMap<String, String> selectedColumns;

    if (requestedColumns == null || requestedColumns.isEmpty()) {
      selectedColumns = allFields;
    } else {
      selectedColumns = new HashMap<>();
      for (String columnName : requestedColumns) {
        String type = allFields.get(columnName);
        if (type != null) {
          selectedColumns.put(columnName, type);
        } else {
          throw new RuntimeException(String.format("Column %s doesn't exist in report %s", columnName, reportType));
        }
      }
    }

    for (Map.Entry<String, String> column : selectedColumns.entrySet()) {
      JsonObject columnJson = new JsonObject();
      columnJson.addProperty("columnName", column.getKey());
      columnJson.addProperty("isNullable", true);

      JsonObject typeJson = new JsonObject();
      String typeString = column.getValue().toLowerCase();
      JsonElementConversionFactory.Type acceptedType = typeConversionMap.get(typeString);
      if (acceptedType == null) {
        acceptedType = JsonElementConversionFactory.Type.STRING;
      }
      typeJson.addProperty("type", acceptedType.toString());

      columnJson.add("dataType", typeJson);
      schema.add(columnJson);
    }

    return schema.toString();
  }

  private static HashMap<String, String> getReportFields(AdWordsSession rootSession,
      ReportDefinitionReportType reportType)
      throws RemoteException {
    try {
      AdWordsServices adWordsServices = new AdWordsServices();

      ReportDefinitionServiceInterface reportDefinitionService =
          adWordsServices.get(rootSession, ReportDefinitionServiceInterface.class);

      ReportDefinitionField[] reportDefinitionFields = reportDefinitionService.getReportFields(
          com.google.api.ads.adwords.axis.v201609.cm.ReportDefinitionReportType.fromString(reportType.toString()));
      HashMap<String, String> fields = new HashMap<>();
      for (ReportDefinitionField field : reportDefinitionFields) {
        fields.put(field.getFieldName(), field.getFieldType());
      }
      return fields;
    } catch (RemoteException e) {
      LOG.error(e.getMessage());
      throw new RuntimeException(e);
    }
  }
}
