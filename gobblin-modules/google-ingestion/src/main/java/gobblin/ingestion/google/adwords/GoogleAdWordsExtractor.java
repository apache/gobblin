package gobblin.ingestion.google.adwords;

import java.io.IOException;
import java.rmi.RemoteException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

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
import lombok.extern.slf4j.Slf4j;

import gobblin.configuration.WorkUnitState;
import gobblin.converter.avro.JsonElementConversionFactory;
import gobblin.ingestion.google.AsyncIteratorWithDataSink;
import gobblin.source.extractor.DataRecordException;
import gobblin.source.extractor.Extractor;
import gobblin.source.extractor.extract.LongWatermark;

import static gobblin.ingestion.google.webmaster.GoogleWebmasterExtractor.dateFormatter;


@Slf4j
public class GoogleAdWordsExtractor implements Extractor<String, String[]> {
  private final static Splitter splitter = Splitter.on(",").omitEmptyStrings().trimResults();
  private final WorkUnitState _state;
  private final GoogleAdWordsExtractorIterator _iterator;
  private final DateTime _startDate;
  private final DateTime _expectedEndDate;
  private JsonArray _schema;
  private final static DateTimeFormatter watermarkFormatter = DateTimeFormat.forPattern("yyyyMMddHHmmss");
  private final static HashMap<String, JsonElementConversionFactory.Type> typeConversionMap = new HashMap<>();
  private boolean _successful = false;

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
    long lowWatermark = state.getWorkunit().getLowWatermark(LongWatermark.class).getValue();
    _startDate = watermarkFormatter.parseDateTime(Long.toString(lowWatermark));
    long highWatermark = state.getWorkunit().getExpectedHighWatermark(LongWatermark.class).getValue();
    _expectedEndDate = watermarkFormatter.parseDateTime(Long.toString(highWatermark));

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

    HashMap<String, String> allFields = downloadReportFields(rootSession, reportType);
    try {
      _schema = createSchema(allFields, columnNames);
      log.info(String.format("Schema for report %s: %s", reportType, _schema));
    } catch (IOException e) {
      throw new RuntimeException(String.format("Failed downloading report %s", reportType), e);
    }

    String startDate = dateFormatter.print(_startDate);
    String endDate = dateFormatter.print(_expectedEndDate);
    GoogleAdWordsReportDownloader downloader =
        new GoogleAdWordsReportDownloader(rootSession, _state, startDate, endDate, reportType, dateRangeType, _schema);

    _iterator = new GoogleAdWordsExtractorIterator(downloader, getConfiguredAccounts(rootSession, state), state);
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
    log.info(
        String.format("Found %d available accounts for your master account %s", available.size(), masterCustomerId));

    if (exactAccounts != null) {
      Sets.SetView<String> difference = Sets.difference(exactAccounts, available);
      if (difference.isEmpty()) {
        return exactAccounts;
      } else {
        String msg = String
            .format("The following accounts configured in the exact list don't exist under master account %s: %s",
                masterCustomerId, Joiner.on(",").join(difference));
        log.error(msg);
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
    JsonArray updatedSchema = new JsonArray();
    for (int i = 0; i < _schema.size(); i++) {
      updatedSchema.add(_schema.get(i));
    }
    //add extra columns(AccountId) at the end of the original schema
    updatedSchema.add(createColumnJson("extraAccId", false, JsonElementConversionFactory.Type.STRING));
    return updatedSchema.toString();
  }

  @Override
  public String[] readRecord(@Deprecated String[] reuse)
      throws DataRecordException, IOException {
    while (_iterator.hasNext()) {
      return _iterator.next();
    }
    _successful = true;
    return null;
  }

  @Override
  public long getExpectedRecordCount() {
    if (_successful) {
      //Any positive number will be okay.
      //Need to add this because of this commit:
      //76ae45a by ibuenros on 12/20/16 at 11:34AM Query based source will reset to low watermark if previous run did not process any data for that table.
      return 1;
    }
    return 0;
  }

  @Override
  public long getHighWatermark() {
    throw new UnsupportedOperationException("This method has been deprecated!");
  }

  @Override
  public void close()
      throws IOException {
    if (_successful) {
      log.info(String
          .format("Successfully downloaded %s reports for [%s, %s).", _state.getProp(GoogleAdWordsSource.KEY_REPORT),
              dateFormatter.print(_startDate), dateFormatter.print(_expectedEndDate)));
      _state.setActualHighWatermark(_state.getWorkunit().getExpectedHighWatermark(LongWatermark.class));
    } else {
      log.error(String
          .format("Failed downloading %s reports for [%s, %s).", _state.getProp(GoogleAdWordsSource.KEY_REPORT),
              dateFormatter.print(_startDate), dateFormatter.print(_expectedEndDate)));
    }
  }

  static JsonArray createSchema(HashMap<String, String> allFields, List<String> requestedColumns)
      throws IOException {
    JsonArray schema = new JsonArray();
    TreeMap<String, String> selectedColumns;

    if (requestedColumns == null || requestedColumns.isEmpty()) {
      selectedColumns = new TreeMap<>(allFields);
    } else {
      selectedColumns = new TreeMap<>();
      for (String columnName : requestedColumns) {
        String type = allFields.get(columnName);
        if (type == null) {
          throw new IOException(String.format("Column %s doesn't exist", columnName));
        }
        selectedColumns.put(columnName, type);
      }
    }

    for (Map.Entry<String, String> column : selectedColumns.entrySet()) {
      String typeString = column.getValue().toLowerCase();
      JsonElementConversionFactory.Type acceptedType = typeConversionMap.get(typeString);
      if (acceptedType == null) {
        acceptedType = JsonElementConversionFactory.Type.STRING;
      }
      schema.add(createColumnJson(column.getKey(), true, acceptedType));
    }
    return schema;
  }

  private static JsonObject createColumnJson(String columnName, boolean isNullable,
      JsonElementConversionFactory.Type columnType) {
    JsonObject columnJson = new JsonObject();
    columnJson.addProperty("columnName", columnName);
    columnJson.addProperty("isNullable", isNullable);

    JsonObject typeJson = new JsonObject();
    typeJson.addProperty("type", columnType.toString());
    columnJson.add("dataType", typeJson);

    return columnJson;
  }

  private static HashMap<String, String> downloadReportFields(AdWordsSession rootSession,
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
      log.error(e.getMessage());
      throw new RuntimeException(e);
    }
  }
}
