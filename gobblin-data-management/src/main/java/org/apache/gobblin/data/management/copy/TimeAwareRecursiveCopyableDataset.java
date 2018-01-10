package org.apache.gobblin.data.management.copy;

import com.google.common.collect.Lists;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

public class TimeAwareRecursiveCopyableDataset extends RecursiveCopyableDataset {
  private static final String CONFIG_PREFIX = CopyConfiguration.COPY_PREFIX + ".recursive";
  public static final String DATE_PATTERN_KEY=CONFIG_PREFIX + ".date.pattern";
  public static final String LOOKBACK_DAYS_KEY=CONFIG_PREFIX + ".lookback.days";

  private final Integer lookbackDays;
  private final String datePattern;

  public TimeAwareRecursiveCopyableDataset(FileSystem fs, Path rootPath, Properties properties, Path glob) {
    super(fs, rootPath, properties, glob);
    this.lookbackDays = Integer.parseInt(properties.getProperty(LOOKBACK_DAYS_KEY));
    this.datePattern = properties.getProperty(DATE_PATTERN_KEY);
  }

  public static class DateRangeIterator implements Iterator {
    private LocalDateTime startDate;
    private LocalDateTime endDate;
    private boolean isDatePatternHourly;

    public DateRangeIterator(LocalDateTime startDate, LocalDateTime endDate, String datePattern) {
      this.startDate = startDate;
      this.endDate = endDate;
      this.isDatePatternHourly = datePattern.contains("HH")? true: false;
    }

    @Override
    public boolean hasNext() {
      return !startDate.isAfter(endDate);
    }

    @Override
    public LocalDateTime next() {
      LocalDateTime dateTime = startDate;
      startDate = isDatePatternHourly? startDate.plusHours(1) : startDate.plusDays(1);
      return dateTime;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }

  @Override
  protected List<FileStatus> getFilesAtPath(FileSystem fs, Path path, PathFilter fileFilter) throws IOException {
    DateTimeFormatter format = DateTimeFormatter.ofPattern(datePattern);
    LocalDateTime endDate = LocalDateTime.now();
    LocalDateTime startDate = endDate.minusDays(lookbackDays);
    DateRangeIterator dateRangeIterator = new DateRangeIterator(startDate,endDate,datePattern);
    List<FileStatus> fileStatuses = Lists.newArrayList();
    while (dateRangeIterator.hasNext()) {
      Path pathWithDateTime = new Path(path,dateRangeIterator.next().format(format));
      fileStatuses.addAll(super.getFilesAtPath(fs,pathWithDateTime,fileFilter));
    }
    return fileStatuses;
  }
}
