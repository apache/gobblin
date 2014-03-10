package com.linkedin.uif.source.extractor.watermark;

import java.util.HashMap;
import com.linkedin.uif.source.extractor.extract.BaseExtractor;

public interface Watermark {
    /**
     * Condition statement with the water mark value using the operator. Example (last_updated_ts >= 2013-01-01 00:00:00
     *
     * @param extractor
     * @param water mark value
     * @param relational operator between water mark column and value
     * @return condition statement
     */
	public String getWatermarkCondition(BaseExtractor extractor, long watermarkValue, String operator);
	
    /**
     * Get partitions for the given range
     *
     * @param low water mark value
     * @param high water mark value
     * @param partition interval(in hours or days)
     * @param maximum number of partitions
     * @return partitions
     */
	public HashMap<Long, Long> getIntervals(long lowWatermarkValue, long highWatermarkValue, int partitionInterval, int maxIntervals);
	
    /**
     * Get number of seconds or hour or days or simple number that needs to be added for the successive water mark
     * @return delta value in seconds or hour or days or simple number
     */
	public int getDeltaNumForNextWatermark();
}
