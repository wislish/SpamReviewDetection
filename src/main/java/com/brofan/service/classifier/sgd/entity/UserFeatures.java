package com.brofan.service.classifier.sgd.entity;

public class UserFeatures {
	private String uid;
	private float rd;
	private float c;
	private float etf;
	private boolean sr;
	private int spam;

	public String getUid() {
		return uid;
	}
	public void setUid(String uid) {
		this.uid = uid;
	}
	public float getRD() {
		return rd;
	}
	public void setRD(float rd) {
		this.rd = rd;
	}
	public float getC() {
		return c;
	}
	public void setC(float c) {
		this.c = c;
	}
	public float getETF() {
		return etf;
	}
	public void setETF(float etf) {
		this.etf = etf;
	}
	public boolean isSR() {
		return sr;
	}
	public void setSR(boolean sr) {
		this.sr = sr;
	}

	public int isSpam() {
		return spam;
	}
	public void setSpam(int spam) {
		this.spam = spam;
	}
}