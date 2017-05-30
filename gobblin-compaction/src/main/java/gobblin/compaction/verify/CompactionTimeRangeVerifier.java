package gobblin.compaction.verify;

import gobblin.compaction.dataset.TimeBasedSubDirDatasetsFinder;
import gobblin.compaction.mapreduce.MRCompactor;
import gobblin.compaction.parser.CompactionPathParser;
import gobblin.configuration.State;
import gobblin.dataset.FileSystemDataset;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Period;
import org.joda.time.format.PeriodFormatter;
import org.joda.time.format.PeriodFormatterBuilder;

/**
 * A simple class which verify current dataset belongs to a specific time range. Will skip to do
 * compaction if dataset is not in a correct time range.
 */

@Slf4j
@AllArgsConstructor
public class CompactionTimeRangeVerifier implements CompactionVerifier<FileSystemDataset> {
  public final static String COMPACTION_VERIFIER_TIME_RANGE = COMPACTION_VERIFIER_PREFIX + "time-range";

  protected State state;

  public boolean verify (FileSystemDataset dataset) {

    try {
      CompactionPathParser.CompactionParserResult result = new CompactionPathParser(state).parse(dataset);
      DateTime folderTime = result.getTime();
      DateTimeZone timeZone = DateTimeZone.forID(this.state.getProp(MRCompactor.COMPACTION_TIMEZONE, MRCompactor.DEFAULT_COMPACTION_TIMEZONE));
      DateTime current = new DateTime(timeZone);
      PeriodFormatter formatter = new PeriodFormatterBuilder().appendMonths().appendSuffix("m").appendDays().appendSuffix("d").appendHours()
              .appendSuffix("h").toFormatter();

      // get earliest time
      String maxTimeAgoStr = this.state.getProp(TimeBasedSubDirDatasetsFinder.COMPACTION_TIMEBASED_MAX_TIME_AGO, TimeBasedSubDirDatasetsFinder.DEFAULT_COMPACTION_TIMEBASED_MAX_TIME_AGO);
      Period maxTimeAgo = formatter.parsePeriod(maxTimeAgoStr);
      DateTime earliest = current.minus(maxTimeAgo);

      // get latest time
      String minTimeAgoStr = this.state.getProp(TimeBasedSubDirDatasetsFinder.COMPACTION_TIMEBASED_MIN_TIME_AGO, TimeBasedSubDirDatasetsFinder.DEFAULT_COMPACTION_TIMEBASED_MIN_TIME_AGO);
      Period minTimeAgo = formatter.parsePeriod(minTimeAgoStr);
      DateTime latest = current.minus(minTimeAgo);

      if (earliest.isBefore(folderTime) && latest.isAfter(folderTime)) {
        log.info("{} falls in the user defined time range", dataset.datasetRoot());
        return true;
      }
    } catch (Exception e) {
      log.error("{} cannot be verified because of {}", dataset.datasetRoot(), e.toString());
    }
    return false;
  }

  public String getName() {
    return COMPACTION_VERIFIER_TIME_RANGE;
  }

  public boolean isRetriable () {
    return false;
  }
}
