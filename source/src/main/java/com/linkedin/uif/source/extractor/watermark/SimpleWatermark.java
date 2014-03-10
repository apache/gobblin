package com.linkedin.uif.source.extractor.watermark;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;

import com.linkedin.uif.source.extractor.extract.BaseExtractor;

public class SimpleWatermark implements Watermark {
	private static final SimpleDateFormat INPUTFORMAT = new SimpleDateFormat("yyyyMMddHHmmss");
	private static final SimpleDateFormat OUTPUTFORMAT = new SimpleDateFormat("yyyyMMdd");
	private static final int deltaForNextWatermark = 1;
    private String watermarkColumn;
    
	public SimpleWatermark(String watermarkColumn, String watermarkFormat) {
		this.watermarkColumn = watermarkColumn;
	}

	@Override
	public String getWatermarkCondition(BaseExtractor extractor, long watermarkValue, String operator) {
		return this.watermarkColumn + operator +  watermarkValue;
	}
	
	@Override
	public int getDeltaNumForNextWatermark() {
		return deltaForNextWatermark;
	}

	@Override
	public HashMap<Long, Long> getIntervals(long lowWatermarkValue, long highWatermarkValue, int partitionInterval, int maxIntervals) {
		HashMap<Long, Long> intervalMap = new HashMap<Long, Long>();
		long nextNum;
		if(partitionInterval < 1) {
			partitionInterval = 1;
		}
		
		int interval = this.getInterval(highWatermarkValue - lowWatermarkValue, partitionInterval, maxIntervals);
		if(interval == 0) {
			return intervalMap;
		}
		
		long startNum = lowWatermarkValue;
		long endNum = highWatermarkValue;
		while(startNum <= endNum) {
			nextNum = startNum+(interval-1);
			intervalMap.put(startNum, (nextNum <= endNum ? nextNum : endNum));
			startNum = nextNum+deltaForNextWatermark;
		}
		return intervalMap;
	}
	
	private int getInterval(long diff, int partitionInterval, int maxIntervals) {	
		if(diff == 0) {
			return 0;
		}
		long totalIntervals = (int) Math.ceil((float)diff/(partitionInterval));
		if(totalIntervals > maxIntervals) {
			partitionInterval = (int) Math.ceil((float)diff/maxIntervals);
		}
		return partitionInterval;
	}
}
