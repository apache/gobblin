package gobblin.source.extractor.extract.google.webmaster;

import com.google.common.base.Splitter;
import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.WorkUnitState;
import gobblin.source.extractor.DataRecordException;
import gobblin.source.extractor.Extractor;
import gobblin.source.extractor.extract.LongWatermark;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Queue;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static gobblin.configuration.ConfigurationKeys.*;


public class GoogleWebmasterExtractor implements Extractor<String, String[]> {
  private final static Logger LOG = LoggerFactory.getLogger(GoogleWebmasterExtractor.class);
  private final static Splitter splitter = Splitter.on(",").omitEmptyStrings().trimResults();
  private final String _schema;
  private final WorkUnitState _wuState;
  private final DateTimeFormatter _watermarkFormatter;
  private Queue<GoogleWebmasterExtractorIterator> _iterators = new ArrayDeque<>();
  private final DateTime _currentDate;
  private boolean _successful = false;

  public GoogleWebmasterExtractor(WorkUnitState wuState) {
    this._wuState = wuState;
    _watermarkFormatter = DateTimeFormat.forPattern("yyyyMMddHHmmss")
        .withZone(DateTimeZone.forID(wuState.getProp(SOURCE_TIMEZONE, DEFAULT_SOURCE_TIMEZONE)));
    _currentDate = _watermarkFormatter.parseDateTime(
        Long.toString(wuState.getWorkunit().getLowWatermark(LongWatermark.class).getValue()));

    String date = DateTimeFormat.forPattern("yyyy-MM-dd").print(_currentDate);
    LOG.info("Creating GoogleWebmasterExtractor for " + date);

    _schema = wuState.getWorkunit().getProp(ConfigurationKeys.SOURCE_SCHEMA);

    int pageLimit = Integer.parseInt(wuState.getProp(GoogleWebMasterSource.KEY_REQUEST_PAGE_LIMIT));
    int queryLimit = Integer.parseInt(wuState.getProp(GoogleWebMasterSource.KEY_REQUEST_QUERY_LIMIT));

    try {
      String property = wuState.getProp(GoogleWebMasterSource.KEY_PROPERTY);
      String credential = wuState.getProp(GoogleWebMasterSource.KEY_CREDENTIAL_LOCATION);
      Iterable<String> filters = splitter.split(wuState.getProp(GoogleWebMasterSource.KEY_PREDEFINED_FILTERS));
      String appName = wuState.getProp(ConfigurationKeys.SOURCE_ENTITY);
      String scope = wuState.getProp(GoogleWebMasterSource.KEY_API_SCOPE);
      GoogleWebmasterClientImpl webmaster =
          new GoogleWebmasterClientImpl(property, credential, appName, scope, filters);

      Iterable<String> countries = splitter.split(wuState.getProp(GoogleWebMasterSource.KEY_COUNTRIES));
      for (String country : countries) {
        _iterators.add(new GoogleWebmasterExtractorIterator(webmaster, date,
            GoogleWebmasterFilter.Country.valueOf(country.toUpperCase()), pageLimit, queryLimit));
      }
    } catch (IOException e) {
      LOG.error("Failed to create Google webmaster API client: " + e.getMessage());
    }
  }

  @Override
  public String getSchema() throws IOException {
    return this._schema;
  }

  @Override
  public String[] readRecord(@Deprecated String[] reuse) throws DataRecordException, IOException {
    while (!_iterators.isEmpty()) {
      GoogleWebmasterExtractorIterator iterator = _iterators.peek();
      if (iterator.hasNext()) {
        return iterator.next();
      }
      _iterators.remove();
    }
//    if (1 == 1) {
//      throw new DataRecordException("Fail me!!! at " + _currentDate.toString());
//    }
    _successful = true;
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
  public void close() throws IOException {
    if (_successful) {
      LOG.info("Successfully finished fetching data from Google Search Console for " + _currentDate.toString());
      _wuState.setActualHighWatermark(
          new LongWatermark(Long.parseLong(_watermarkFormatter.print(_currentDate.plusDays(1)))));
    } else {
      LOG.warn("Had problems fetching all data from Google Search Console for " + _currentDate.toString());
    }
  }
}
