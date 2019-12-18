package com.sd.etl.util;

public enum TabName {
	
    SD("sd");
	
	private String tabName;

	TabName(String tabName) {
		this.tabName = tabName;
	}

	public String toString() {
		return this.tabName;
	}

}
