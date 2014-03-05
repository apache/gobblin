package com.linkedin.uif.source.extractor.watermark;

import com.google.common.base.Strings;
import com.linkedin.uif.source.extractor.extract.BaseExtractor;

public class WatermarkPredicate {
	private static final String DEFAULT_WATERMARK_VALUE_FORMAT = "yyyyMMddHHmmss";
	private static final long DEFAULT_WATERMARK_VALUE = -1;
	private BaseExtractor extractor;
	private String watermarkColumn;
	private Watermark watermark;
	private String watermarkSourceFormat;
	
	public String getWatermarkSourceFormat() {
		return this.watermarkSourceFormat;
	}

	public WatermarkPredicate(BaseExtractor extractor, String watermarkColumn, WatermarkType watermarkType) {
		super();
		this.extractor = extractor;
		this.watermarkColumn = watermarkColumn;
		this.watermarkSourceFormat = this.extractor.getWatermarkSourceFormat(watermarkType);
		
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
	
	public Predicate getPredicate(long watermarkValue, String operator) {
		String condition = "";
		if(watermarkValue != DEFAULT_WATERMARK_VALUE) {
			condition = this.watermark.getWatermarkCondition(extractor, watermarkValue, operator);
		}
				
		if (Strings.isNullOrEmpty(watermarkColumn) || condition.equals("")) {
			return null;
		}
		return new Predicate(this.watermarkColumn, watermarkValue, condition, this.watermarkSourceFormat);
	}

}
