package com.sd.etl.util;

import org.apache.spark.sql.Dataset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sd.etl.load.CassandraTabWriter;
import com.sd.etl.transformation.ITabMapper;

/**
 * Thread used to perform the ETL and store the result in Cassandra.
 * @author MBENTA
 *
 * @param <T> - ETL Dataset type
 */
public class MapperSparkJob<T> {

	/** Logger. */
	private static final Logger LOGGER = LoggerFactory.getLogger(MapperSparkJob.class);
    /** ETL mapper. */
	private ITabMapper<T> tabMapper;
	/** Cassandra persist utility. */
	private CassandraTabWriter<T> cassandraTabWriter;

	/**
	 * Constructor.
	 * @param tabMapper - ITabMapper instance
	 */
	public MapperSparkJob(ITabMapper<T> tabMapper) {
		super();
		this.tabMapper = tabMapper;
		cassandraTabWriter = new CassandraTabWriter<T>(tabMapper.getTabName());
	}

	public void run() {
		String tabName = tabMapper.getTabName().name();
		long startTime = System.currentTimeMillis();		
		LOGGER.info("Thread start spark job for tab " + tabName);
		tabMapper.getDataset();
		long endTime = System.currentTimeMillis();
		LOGGER.info("Thread finish spark job for tab " + tabName + " in " + (endTime - startTime) + " millis");
	}


}
