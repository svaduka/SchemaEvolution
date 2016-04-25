package com.schemaevolution;

import org.apache.hadoop.io.WritableComparable;

public interface DataStorageFormat {
	
	public WritableComparable<?> parseData(final String inputData);

}
