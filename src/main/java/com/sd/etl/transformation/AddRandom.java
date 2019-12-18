package com.sd.etl.transformation;

import java.io.Serializable;


import org.apache.spark.sql.api.java.UDF1;

public class AddRandom implements UDF1<Integer, Integer>, Serializable{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public static final String ADD_RANDOM = "addRandom";
	
	private static final int Min = 40;
	private static final int Max = 500;
	

	@Override
	public Integer call(Integer nr) throws Exception {
		int x = Min + (int)(Math.random() * ((Max - Min) + 1));
		return nr  + x;
	}}
