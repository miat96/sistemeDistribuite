package com.sd.etl.pojo;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonRawValue;

public class SDEtlTab implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private String part;
	private String json;

	public String getPart() {
		return part;
	}

	public void setPart(String part) {
		this.part = part;
	}

	@JsonRawValue
	public String getJson() {
		return json;
	}

	public void setJson(String json) {
		this.json = json;
	}

}
