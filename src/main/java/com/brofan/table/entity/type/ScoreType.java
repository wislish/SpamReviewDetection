package com.brofan.table.entity.type;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class ScoreType {
	private String scoreName;
	private boolean calSD;

	// data column
	private byte[] dataCol = null;
	
	// feature column
	private byte[] meanCol = null;
	private byte[] sdCol = null;
	
	public ScoreType(String name, boolean calSD) {
		this.scoreName = name;
		this.calSD = calSD;
		getColName();
	}
	
	protected void getColName() {
		
		StringBuffer name = new StringBuffer(scoreName);
		
		if (dataCol == null) {
			
			// construct data column name

			dataCol = Bytes.toBytes(name.toString());
			
		}
		
		if (meanCol == null) {
			
			// construct mean column name
			// example: star_mean
			StringBuffer mean = new StringBuffer(name);
			
			mean.append("_mean");
			meanCol = Bytes.toBytes(mean.toString());
		}
		
		if (calSD) {
			if (sdCol == null) {
				
				// construct standard deviation column name
				// example: star_sd
				StringBuffer sd = new StringBuffer(name);
				
				sd.append("_sd");
				sdCol = Bytes.toBytes(sd.toString());
			}
		}
	}
	
	public void putData(Put put, byte[] family, int rate) {
		put.add(family, dataCol, Bytes.toBytes(rate));
	}
	
	public void putMean(Put put, byte[] family, float value) {
		put.add(family, meanCol, Bytes.toBytes(value));
	}
	
	public void putSD(Put put, byte[] family, float value) {
		if (calSD) {
			put.add(family, sdCol, Bytes.toBytes(value));
		}
	}
	
	public byte[] getDataCol() {
		return dataCol;
	}

	public byte[] getMeanCol() {
		return meanCol;
	}

	public byte[] getSdCol() {
		if (calSD) {
			return sdCol;
		}
		return null;
	}
	
	public String getScoreName() {
		return scoreName;
	}
	
	public boolean isCalSD() {
		return calSD;
	}

	public void setCalSD(boolean calSD) {
		this.calSD = calSD;
	}

	public void setScoreName(String scoreName) {
		this.scoreName = scoreName;
	}
}