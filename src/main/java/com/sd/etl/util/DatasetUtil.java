package com.sd.etl.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;

public class DatasetUtil {
		
	private static final String FILE_BASE_PATH = "tabs_queries/";

	
	public static Dataset<Row> renameColumns(Dataset<Row> dataset, String mapFileLocation) {
		final String filePath = FILE_BASE_PATH.concat(mapFileLocation);

		Properties renameMap = new Properties();
		
		InputStream in = DatasetUtil.class.getClassLoader().getResourceAsStream(filePath);
		try {
			renameMap.load(in);
			for (Object keyObj: renameMap.keySet()) {
				String key = keyObj.toString();
				dataset = dataset.withColumnRenamed(key, renameMap.getProperty(key));
			}
		} catch (IOException e1) {
			e1.printStackTrace();
			if (in != null) {
				try {
					in.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		
		return dataset;		
	}
	
	public static String emptyIfNull(Object value) {
		return null == value ? "" : value.toString();
	}
}
