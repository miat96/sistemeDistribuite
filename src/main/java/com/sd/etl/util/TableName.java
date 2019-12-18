package com.sd.etl.util;

public enum TableName {
	SD("sd"),
	SD2("sd2");
	
	private String tableName;

	private TableName(String tableName) {
		this.tableName = tableName;
	}
	
	public String toString() {
		return this.tableName;
	}

}
