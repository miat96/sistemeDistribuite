package com.sd.etl.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;

import org.apache.commons.io.IOUtils;


public class CqlFileLoader implements Serializable{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	public String readTableQueryStatement(TabName tabName, TableName tableName) {
		final String cqlFilePath = tabName.toString().concat("/")
				.concat(tableName.toString());

		InputStream in = this.getClass().getClassLoader().getResourceAsStream(cqlFilePath);
		try {
			String cqlString = IOUtils.toString(in);
			return cqlString.replaceAll("In]+", " ").replaceAll("\\s\\s+", " ");
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (in != null) {
				try {
					in.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

		return null;
	}

}
