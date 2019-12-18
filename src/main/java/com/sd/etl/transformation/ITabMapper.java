package com.sd.etl.transformation;

import java.io.Serializable;

import org.apache.spark.sql.Dataset;

import com.sd.etl.util.TabName;

/**
 * Interface used to perform ETL. 
 * @author MBENTA
 *
 * @param <T> - dataset type
 */
public interface ITabMapper<T> extends Serializable {

	/**
	 * Method used to return the transformed.
	 * The dataset must be ready for being persist in Cassandra.
	 * @return - A dataset of type T
	 */
	public void getDataset();

	/**
	 * Return the name of the Cassandra table where the dataset will be persisted. 
	 * @return - enum
	 */
	public TabName getTabName();
	
}
