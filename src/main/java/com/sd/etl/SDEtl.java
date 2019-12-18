package com.sd.etl;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import com.sd.etl.transformation.SDTabMapper;
import com.sd.etl.util.MapperSparkJob;

public class SDEtl implements Serializable {

	private static final long serialVersionUID = 1L;

	public static void main(String[] args) {
		
		 List<MapperSparkJob<?>> sparkJobList = new ArrayList<>();
	     sparkJobList.add(new MapperSparkJob<>(new SDTabMapper()));
	     
	     for (MapperSparkJob<?> mapperSparkJob : sparkJobList) {
	            mapperSparkJob.run();
	     }

	}

}
