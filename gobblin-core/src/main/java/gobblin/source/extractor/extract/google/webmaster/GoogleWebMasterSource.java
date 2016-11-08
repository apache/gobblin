package gobblin.source.extractor.extract.google.webmaster;

import gobblin.configuration.WorkUnitState;
import gobblin.source.extractor.Extractor;
import gobblin.source.extractor.extract.QueryBasedSource;
import java.io.IOException;


public class GoogleWebMasterSource extends QueryBasedSource<String, String[]> {

  public static final String KEY_CREDENTIAL_LOCATION = "source.google_webmasters.credential.location";
  public static final String KEY_PROPERTY = "source.google_webmasters.property";
  public static final String KEY_PREDEFINED_FILTERS = "source.google_webmasters.predefined_filters";
  public static final String KEY_API_SCOPE = "source.google.api.scope";
  public static final String KEY_REQUEST_DATE = "source.google_webmasters.request.date";
  public static final String KEY_COUNTRIES = "source.google_webmasters.request.countries";
  public static final String KEY_REQUEST_PAGE_LIMIT = "source.google_webmasters.request.page_limit";
  public static final String KEY_REQUEST_QUERY_LIMIT = "source.google_webmasters.request.query_limit";

  @Override
  public Extractor<String, String[]> getExtractor(WorkUnitState state) throws IOException {
    return new GoogleWebmasterExtractor(state);
  }
}
