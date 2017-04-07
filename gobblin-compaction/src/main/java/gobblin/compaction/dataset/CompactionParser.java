package gobblin.compaction.dataset;

import org.apache.commons.lang.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import com.google.common.base.Preconditions;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import gobblin.compaction.mapreduce.MRCompactor;
import gobblin.configuration.State;


/**
 * Parse the {@link CompactionPartition} to some format of result
 * that downstream user can utilize.
 *
 * {@link CompactionParserResult} is a return result of {@link CompactionParser#parse(CompactionPartition)}
 */
@AllArgsConstructor
public class CompactionParser {
  State state;

  public static class CompactionParserResult {
    @Getter @Setter
    private  String srcBaseDir;
    @Getter @Setter
    private  String dstBaseDir;
    @Getter @Setter
    private  String srcSubDir;
    @Getter @Setter
    private  String dstSubDir;

    @Getter
    private DateTime time;
    @Getter
    private String timeString;
    @Getter
    private String datasetName;
  }

  public CompactionParserResult parse (CompactionPartition partition) {

    CompactionParserResult result = new CompactionParserResult();
    result.srcBaseDir = getSrcBaseDir (state);
    result.srcSubDir  = getSrcSubDir  (state);
    result.dstBaseDir = getDstBaseDir (state);
    result.dstSubDir  = getDstSubDir  (state);

    parseTimeAndDatasetName(partition, result);

    return result;
  }

  private void parseTimeAndDatasetName (CompactionPartition partition, CompactionParserResult rst) {
    String commonBase = rst.getSrcBaseDir();
    String fullPath = partition.getPath().toString();
    int startPos = fullPath.indexOf(commonBase) + commonBase.length();
    String relative = StringUtils.removeStart(fullPath.substring(startPos), "/");

    int delimiterStart = StringUtils.indexOf(relative, rst.getSrcSubDir());
    if (delimiterStart == -1) {
      throw new StringIndexOutOfBoundsException();
    }
    int delimiterEnd = relative.indexOf("/", delimiterStart);
    String datasetName = StringUtils.removeEnd(relative.substring(0, delimiterStart), "/");
    String timeString = StringUtils.removeEnd(relative.substring(delimiterEnd + 1), "/");
    rst.datasetName = datasetName;
    rst.timeString = timeString;
    rst.time = getTime (timeString);
  }

  private DateTime getTime (String timeString) {
    DateTimeZone timeZone = DateTimeZone.forID(MRCompactor.DEFAULT_COMPACTION_TIMEZONE);
    int splits = StringUtils.countMatches(timeString, "/");
    String timePattern = "";
    if (splits == 3) {
      timePattern = "YYYY/MM/dd/HH";
    } else if (splits == 2) {
      timePattern = "YYYY/MM/dd";
    }
    DateTimeFormatter timeFormatter = DateTimeFormat.forPattern(timePattern).withZone(timeZone);
    return timeFormatter.parseDateTime (timeString);
  }

  private String getSrcBaseDir(State state) {
    Preconditions.checkArgument(state.contains(MRCompactor.COMPACTION_INPUT_DIR),
        "Missing required property " + MRCompactor.COMPACTION_INPUT_DIR);
    return state.getProp(MRCompactor.COMPACTION_INPUT_DIR);
  }

  private String getSrcSubDir(State state) {
    Preconditions.checkArgument(state.contains(MRCompactor.COMPACTION_INPUT_SUBDIR),
        "Missing required property " + MRCompactor.COMPACTION_INPUT_SUBDIR);
    return state.getProp(MRCompactor.COMPACTION_INPUT_SUBDIR);
  }

  private String getDstBaseDir(State state) {
    Preconditions.checkArgument(state.contains(MRCompactor.COMPACTION_DEST_DIR),
        "Missing required property " + MRCompactor.COMPACTION_DEST_DIR);
    return state.getProp(MRCompactor.COMPACTION_DEST_DIR);
  }

  private String getDstSubDir(State state) {
    Preconditions.checkArgument(state.contains(MRCompactor.COMPACTION_DEST_SUBDIR),
        "Missing required property " + MRCompactor.COMPACTION_DEST_SUBDIR);
    return state.getProp(MRCompactor.COMPACTION_DEST_SUBDIR);
  }
}
