package com.linkedin.uif.source.extractor.resultset;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * An implementation of RecordSet to return data records through an interator
 *
 * @param <D> type of data record
 */
public class RecordSetList<D> implements RecordSet<D> {
	private List<D> list = new ArrayList<D>();

	@Override
	public Iterator<D> iterator() {
		return this.list.iterator();
	}
	
	@Override
	public void add(D record) {
		list.add(record);
	}
	
	@Override
	public boolean isEmpty() {
		if(this.list.size() == 0) {
			return true;
		}
		return false;
	}
}
