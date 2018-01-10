package org.apache.gobblin.data.management.copy;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class DateRangeIteratorTest {
  @Test
  public void testIterator() {
    LocalDateTime endDate = LocalDateTime.of(2017,1,1,0,0);
    LocalDateTime startDate = endDate.minusHours(2);
    String datePattern = "HH/yyyy/MM/dd";
    DateTimeFormatter format = DateTimeFormatter.ofPattern(datePattern);
    TimeAwareRecursiveCopyableDataset.DateRangeIterator dateRangeIterator = new TimeAwareRecursiveCopyableDataset.DateRangeIterator(startDate,endDate,datePattern);
    LocalDateTime dateTime = dateRangeIterator.next();
    Assert.assertEquals(dateTime.format(format),"22/2016/12/31");
    dateTime = dateRangeIterator.next();
    Assert.assertEquals(dateTime.format(format),"23/2016/12/31");
    dateTime = dateRangeIterator.next();
    Assert.assertEquals(dateTime.format(format),"00/2017/01/01");
    Assert.assertEquals(dateRangeIterator.hasNext(),false);
  }
}