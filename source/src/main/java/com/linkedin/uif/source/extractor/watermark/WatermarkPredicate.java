package com.linkedin.uif.source.extractor.watermark;

import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;
import com.linkedin.uif.source.extractor.extract.BaseExtractor;
import com.linkedin.uif.source.extractor.extract.restapi.RestApiExtractor;

public class WatermarkPredicate {
	private static final String DEFAULT_WATERMARK_VALUE_FORMAT = "yyyyMMddHHmmss";
	private static final long DEFAULT_WATERMARK_VALUE = -1;
	private String watermarkColumn;
	private WatermarkType watermarkType;
	private Watermark watermark;

	   private Logger log = LoggerFactory.getLogger(WatermarkPredicate.class);

	public WatermarkPredicate(String watermarkColumn, WatermarkType watermarkType) {
		super();
		this.watermarkColumn = watermarkColumn;
		this.watermarkType = watermarkType;
		
		switch (watermarkType) {
		case TIMESTAMP:
			this.watermark = new TimestampWatermark(watermarkColumn, DEFAULT_WATERMARK_VALUE_FORMAT);
			break;
		case DATE:
			this.watermark = new DateWatermark(watermarkColumn, DEFAULT_WATERMARK_VALUE_FORMAT);
			break;
		case HOUR:
			this.watermark = new HourWatermark(watermarkColumn, DEFAULT_WATERMARK_VALUE_FORMAT);
			break;
		case SIMPLE:
			this.watermark = new SimpleWatermark(watermarkColumn, DEFAULT_WATERMARK_VALUE_FORMAT);
			break;
		default:
			this.watermark = new SimpleWatermark(watermarkColumn, DEFAULT_WATERMARK_VALUE_FORMAT);
			break;
		}
	}
	
	public Predicate getPredicate(BaseExtractor extractor, long watermarkValue, String operator) {
		String condition = "";
		
		if(watermarkValue != DEFAULT_WATERMARK_VALUE) {
			condition = this.watermark.getWatermarkCondition(extractor, watermarkValue, operator);
		}
		
		if (watermarkColumn == null) {
		    log.info("WMC is null");
		}
		
		if (condition == null) {
            log.info("Condition is null");
        }
		
		Strings.isNullOrEmpty(watermarkColumn);
		
		if (Strings.isNullOrEmpty(watermarkColumn) || condition.equals("")) {
			return null;
		}
		return new Predicate(this.watermarkColumn, watermarkValue, condition, this.getWatermarkSourceFormat(extractor));
	}
	
	public String getWatermarkSourceFormat(BaseExtractor extractor) {
		return extractor.getWatermarkSourceFormat(this.watermarkType);
	}
	
	public HashMap<Long, Long> getPartitions(long lowWatermarkValue, long highWatermarkValue, int partitionInterval, int maxIntervals) {
		return this.watermark.getIntervals(lowWatermarkValue, highWatermarkValue, partitionInterval, maxIntervals);
	}
	
	public int getDeltaNumForNextWatermark() {
		return this.watermark.getDeltaNumForNextWatermark(); 
	}

}
