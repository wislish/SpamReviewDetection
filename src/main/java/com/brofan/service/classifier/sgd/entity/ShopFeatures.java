package com.brofan.service.classifier.sgd.entity;

public class ShopFeatures {
	private String sid;
	private long sr;
	private float bst;
	private float dcv;
	private float rcv;
	private float star;
	private float score1;
	private float score2;
	private float score3;
	private int spam;

	public long getSR() {
		return sr;
	}
	public void setSR(long sr) {
		this.sr = sr;
	}
	public float getBST() {
		return bst;
	}
	public void setBST(float bst) {
		this.bst = bst;
	}
	public float getDCV() {
		return dcv;
	}
	public void setDCV(float dcv) {
		this.dcv = dcv;
	}
	public float getRCV() {
		return rcv;
	}
	public void setRCV(float rcv) {
		this.rcv = rcv;
	}
	public float getStar() {
		return star;
	}
	public void setStar(float star) {
		this.star = star;
	}
	public float getScore1() {
		return score1;
	}
	public void setScore1(float score1) {
		this.score1 = score1;
	}
	public float getScore2() {
		return score2;
	}
	public void setScore2(float score2) {
		this.score2 = score2;
	}
	public float getScore3() {
		return score3;
	}
	public void setScore3(float score3) {
		this.score3 = score3;
	}
	public int hasSpam() {
		return spam;
	}
	public void setSpam(int spam) {
		this.spam = spam;
	}
	public String getSid() {
		return sid;
	}
	public void setSid(String sid) {
		this.sid = sid;
	}
}