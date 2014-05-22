package com.linkedin.uif.source.extractor.watermark;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.uif.source.extractor.extract.BaseExtractor;

public class HourWatermark implements Watermark {
	private static final Logger LOG = LoggerFactory.getLogger(HourWatermark.class);

	// default water mark format(input format) example: 20140301050505
	private static final SimpleDateFormat INPUTFORMAT = new SimpleDateFormat("yyyyMMddHHmmss");
	
	// output format of hour water mark example: 2014030101
	private static final SimpleDateFormat OUTPUTFORMAT = new SimpleDateFormat("yyyyMMddHH");
	private static final int deltaForNextWatermark = 60*60;
    private String watermarkColumn;
    private String watermarkFormat;
    
	public HourWatermark(String watermarkColumn, String watermarkFormat) {
		this.watermarkColumn = watermarkColumn;
		this.watermarkFormat = watermarkFormat;
	}

	@Override
	public String getWatermarkCondition(BaseExtractor extractor, long watermarkValue, String operator) {
		return extractor.getHourPredicateCondition(this.watermarkColumn, watermarkValue, this.watermarkFormat, operator);
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
		Date nextTime;
		Date lowWatermarkDate = this.extractFromTimestamp(Long.toString(lowWatermarkValue));
		Date highWatermarkDate = this.extractFromTimestamp(Long.toString(highWatermarkValue));
		final long lowWatermark = lowWatermarkDate.getTime();
		final long highWatermark = highWatermarkDate.getTime();
		
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
			calendar.add(Calendar.HOUR, interval-1);
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
	
    /**
     * Convert timestamp to hour (yyyymmddHHmmss to yyyymmddHH)
     *
     * @param watermark value
     * @return value in hour format
     */
	private Date extractFromTimestamp(String watermark) {
		Date outDate = null;
		try {
			Date date = INPUTFORMAT.parse(watermark);
			String dateStr = OUTPUTFORMAT.format(date);
			outDate = OUTPUTFORMAT.parse(dateStr);
			
		} catch (ParseException e) {
		    LOG.error(e.getMessage(), e);
		}
		return outDate;
	}
}
