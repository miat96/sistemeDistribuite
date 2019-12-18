package com.sd.etl.processor;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;

import com.sd.etl.transformation.AddRandom;
import com.sd.etl.util.ApplicationProperties;
import com.sd.etl.util.CqlFileLoader;
import com.sd.etl.util.TabName;
import com.sd.etl.util.TableName;

import static com.sd.etl.transformation.AddRandom.ADD_RANDOM;

public class TableProcessor {
	
	private TabName tabName;
	private TableName tableName;
	private CqlFileLoader cqlFileLoader;
	private String cqlStatementQuery;
	private Dataset<Row> dataSetTable;
	private String keyspace;
	
	public TableProcessor(TabName tabName, TableName tableName, CqlFileLoader cqlFileLoader) {
		this(tabName, ApplicationProperties.getInstance().getInputKeyspace(), tableName, cqlFileLoader);
	}

	public TableProcessor(TabName tabName, String keyspace, TableName tableName, CqlFileLoader cqlFileLoader) {
		if (cqlFileLoader == null)
			throw new RuntimeException(CqlFileLoader.class.toString().concat(" object is null!"));

		if (tableName == null)
			throw new RuntimeException(TableName.class.toString().concat(" object is null!"));

		this.cqlFileLoader = cqlFileLoader;
		this.tableName = tableName;
		this.tabName = tabName;
		this.keyspace = keyspace;

		this.initCqlStatementQuery();
		this.initDataSetTable();
	}
	
	public Dataset<Row> processTable() {
		dataSetTable.createOrReplaceTempView(tableName.toString());
		Dataset<Row> tableETL = ApplicationProperties.getInstance().getSparkSession().sql(this.cqlStatementQuery);
		dataSetTable.unpersist();
		
		tableETL.sqlContext().udf().register(ADD_RANDOM, new AddRandom(), DataTypes.IntegerType);

		return tableETL;
	}

	private void initCqlStatementQuery() {
		this.cqlStatementQuery = this.cqlFileLoader.readTableQueryStatement(this.tabName, this.tableName);
	}

	private void initDataSetTable() {
		this.dataSetTable = ApplicationProperties.getInstance().getSparkSession().read()
				.format(ApplicationProperties.getInstance().getFormat())
				.option("keyspace", this.keyspace)
				.option("table", this.tableName.toString()).load();
	}

}
