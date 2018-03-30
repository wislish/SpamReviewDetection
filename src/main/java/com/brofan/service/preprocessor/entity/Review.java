package com.brofan.service.preprocessor.entity;

import java.sql.Timestamp;

import com.brofan.table.entity.type.LogReasonType;

public class Review {
	private String id;
	private String body;
	private String userid;
	private String shopid;
	private int star;
	private int score1;		// taste
	private int score2;		// environment
	private int score3;		// services
	private Timestamp	updatetime;
	private LogReasonType logreason;

	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public String getBody() {
		return body;
	}
	public void setBody(String body) {
		this.body = body;
	}
	public String getUserid() {
		return userid;
	}
	public void setUserid(String userid) {
		this.userid = userid;
	}
	public String getShopid() {
		return shopid;
	}
	public void setShopid(String shopid) {
		this.shopid = shopid;
	}
	public int getStar() {
		return star;
	}
	public void setStar(int star) {
		this.star = star;
	}
	public int getScore1() {
		return score1;
	}
	public void setScore1(int score1) {
		this.score1 = score1;
	}
	public int getScore2() {
		return score2;
	}
	public void setScore2(int score2) {
		this.score2 = score2;
	}
	public int getScore3() {
		return score3;
	}
	public void setScore3(int score3) {
		this.score3 = score3;
	}
	public Timestamp getUpdatetime() {
		return updatetime;
	}
	public void setUpdatetime(Timestamp updatetime) {
		this.updatetime = updatetime;
	}
	public LogReasonType getLogreason() {
		return logreason;
	}
	public void setLogreason(LogReasonType logreason) {
		this.logreason = logreason;
	}
}
