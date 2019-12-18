package com.sd.etl.transformation;

import org.apache.spark.sql.Dataset;

import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;

import com.sd.etl.processor.TableProcessor;
import com.sd.etl.util.CqlFileLoader;
import com.sd.etl.util.TabName;
import com.sd.etl.util.TableName;

import static com.sd.etl.transformation.AddRandom.ADD_RANDOM;

public class SDTabMapper implements ITabMapper<Row>{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	protected CqlFileLoader cqlFileLoader;

	@Override
	public void getDataset() {
		
		cqlFileLoader = new CqlFileLoader();
		
		final TableProcessor tsTableProcessor = new TableProcessor(TabName.SD, TableName.SD, cqlFileLoader);
		final Dataset<Row> ts1 = tsTableProcessor.processTable();
		final Dataset<Row> ts1R = ts1.withColumn("policyID", callUDF(ADD_RANDOM, col("policyID")));
		
		
		final TableProcessor ts2TableProcessor = new TableProcessor(TabName.SD, TableName.SD2, cqlFileLoader);
		final Dataset<Row> ts2 = ts2TableProcessor.processTable();
		final Dataset<Row> ts2R = ts2.withColumn("policyID", callUDF(ADD_RANDOM, col("policyID")));
		
		final Dataset<Row> ts1JoinTs2 = ts1R.join(ts2R, ts1R.col("policyID").equalTo(ts2R.col("policyID")));
		
		ts1JoinTs2.show();
		
	}

	@Override
	public TabName getTabName() {
		return TabName.SD;
		
	}

}
