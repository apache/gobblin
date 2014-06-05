package com.linkedin.uif.source.extractor.watermark;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.uif.source.extractor.extract.QueryBasedExtractor;
import com.linkedin.uif.source.extractor.partition.Partitioner;

public class TimestampWatermark implements Watermark {
	private static final Logger LOG = LoggerFactory.getLogger(TimestampWatermark.class);
	
	// default water mark format(input format) example: 20140301050505
	private static final SimpleDateFormat INPUTFORMAT = new SimpleDateFormat("yyyyMMddHHmmss");

	// output format of timestamp water mark example: 20140301050505
	private static final SimpleDateFormat OUTPUTFORMAT = new SimpleDateFormat("yyyyMMddHHmmss");
	private static final int deltaForNextWatermark = 1;
    private String watermarkColumn;
    private String watermarkFormat;
    
	public TimestampWatermark(String watermarkColumn, String watermarkFormat) {
		this.watermarkColumn = watermarkColumn;
		this.watermarkFormat = watermarkFormat;
	}

	@Override
	public String getWatermarkCondition(QueryBasedExtractor extractor, long watermarkValue, String operator) {
		return extractor.getTimestampPredicateCondition(this.watermarkColumn, watermarkValue, this.watermarkFormat, operator);
	}
	
	@Override
	public int getDeltaNumForNextWatermark() {
		return deltaForNextWatermark;
	}

	@Override
	public HashMap<Long, Long> getIntervals(long lowWatermarkValue, long highWatermarkValue, int partitionInterval, int maxIntervals) {
		HashMap<Long, Long> intervalMap = new HashMap<Long, Long>();
		if(partitionInterval < 1) {
			partitionInterval = 1;
		}
		
		final Calendar calendar = Calendar.getInstance();
		final SimpleDateFormat format  = new SimpleDateFormat("yyyyMMddHHmmss");
		Date nextTime;
		
		final long lowWatermark = this.toEpoch(Long.toString(lowWatermarkValue));
		final long highWatermark = this.toEpoch(Long.toString(highWatermarkValue));
		
		int interval = this.getInterval(highWatermark - lowWatermark, partitionInterval, maxIntervals);
		LOG.info("Recalculated partition interval:"+interval+" hours");
		if(interval == 0) {
			return intervalMap;
		}
		
		Date startTime = new Date(lowWatermark);
		Date endTime = new Date(highWatermark);
		long lwm;
		long hwm;
		while(startTime.getTime() <= endTime.getTime()) {
			lwm = Long.parseLong(INPUTFORMAT.format(startTime));
			calendar.setTime(startTime);
			calendar.add(Calendar.HOUR, interval);
			nextTime = calendar.getTime();
			hwm = Long.parseLong(INPUTFORMAT.format(nextTime.getTime() <= endTime.getTime() ? nextTime : endTime));
			
			intervalMap.put(lwm, hwm);
			
			calendar.add(Calendar.SECOND, deltaForNextWatermark);
			startTime = calendar.getTime();
		}
		return intervalMap;
	}
	
    /**
     * recalculate interval(in hours) if total number of partitions greater than maximum number of allowed partitions
     *
     * @param difference in range
     * @param hour interval (ex: 4 hours)
     * @param Maximum number of allowed partitions
     * @return calculated interval in hours
     */
	private int getInterval(long diffInMilliSecs, int hourInterval, int maxIntervals) {	
		if(diffInMilliSecs == 0) {
			return 0;
		}
		
		int totalHours =  (int) Math.ceil(((float)diffInMilliSecs / (60*60*1000)));
		long totalIntervals = (long) Math.ceil((float)totalHours/hourInterval);
		if(totalIntervals > maxIntervals) {
			hourInterval = (int) Math.ceil((float)totalHours/maxIntervals);
		}
		return hourInterval;
	}
	
	private long toEpoch(String dateTime) {
		Date date = null;
		try {
			date = INPUTFORMAT.parse(dateTime);
		} catch (ParseException e) {
		    LOG.error(e.getMessage(), e);
		}
		return date.getTime();
	}
}