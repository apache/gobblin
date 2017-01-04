package gobblin.ingestion.google.webmaster;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import gobblin.configuration.WorkUnitState;
import gobblin.source.extractor.extract.LongWatermark;


public class GoogleWebMasterSourceWeekly extends GoogleWebMasterSource {
  private static DateTimeFormatter _hmsFormatter = DateTimeFormat.forPattern("yyyyMMddHHmmss");

  @Override
  GoogleWebmasterExtractor createExtractor(WorkUnitState state, Map<String, Integer> columnPositionMap,
      List<GoogleWebmasterFilter.Dimension> requestedDimensions,
      List<GoogleWebmasterDataFetcher.Metric> requestedMetrics) throws IOException {

    long lowWatermark = state.getWorkunit().getLowWatermark(LongWatermark.class).getValue();
    Pair<DateTime, DateTime> taskRange = getTaskRange(lowWatermark);
    Long startDate = Long.valueOf(_hmsFormatter.print(taskRange.getLeft()));
    Long endDate = Long.valueOf(_hmsFormatter.print(taskRange.getRight()));
    return new GoogleWebmasterExtractor(state, startDate, endDate, columnPositionMap, requestedDimensions,
        requestedMetrics);
  }

  /**
   * Return the one-week range from Friday to Thursday.
   *
   * If you are on Sunday, the Thursday is 3 days ago.
   * If not, the Thursday is last Thursday.
   *
   * See tests for more details.
   */
  public static Pair<DateTime, DateTime> getTaskRange(long lowWatermark) {
    DateTime date = _hmsFormatter.parseDateTime(Long.toString(lowWatermark));
    //Monday = 1, Sunday = 7
    int dayOfWeek = date.getDayOfWeek();
    if (dayOfWeek == 7) {
      date = date.plusDays(1); //Go the next Monday.
    } else {
      date = date.minusDays(dayOfWeek - 1); //Go to this Monday.
    }

    DateTime lastMonday = date.minusWeeks(1);
    return Pair.of(lastMonday.minusDays(3), lastMonday.plusDays(3));
  }
}
