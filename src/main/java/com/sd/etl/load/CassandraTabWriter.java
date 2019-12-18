package com.sd.etl.load;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SaveMode;

import com.sd.etl.util.ApplicationProperties;
import com.sd.etl.util.TabName;


public class CassandraTabWriter<T> {

	private TabName tabName;

	public void write(Dataset<T> dataset) {
		dataset.write()
			.format(ApplicationProperties.getInstance().getFormat())
			.option("keyspace", ApplicationProperties.getInstance().getOutputKeyspace())
			.option("table", tabName.toString().toLowerCase())
			.mode(SaveMode.Append)
			.save();
	}

	public CassandraTabWriter(TabName tabName) {
		super();
		this.tabName = tabName;
	}

}
