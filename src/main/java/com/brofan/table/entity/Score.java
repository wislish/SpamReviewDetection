package com.brofan.table.entity;

import org.apache.hadoop.hbase.client.Put;

import com.brofan.service.preprocessor.entity.Review;
import com.brofan.table.entity.type.ScoreType;

// TODO: Needs Refactoring
public final class Score {
	
	private static ScoreType star = new ScoreType("star", true);
	private static ScoreType score1 = new ScoreType("score1", false);
	private static ScoreType score2 = new ScoreType("score2", false);
	private static ScoreType score3 = new ScoreType("score3", false);
	
	private Score() {
	}
	
	public static void putData(Put put, byte[] family, Review review) {
		star.putData(put, family, review.getStar());
		score1.putData(put, family, review.getScore1());
		score2.putData(put, family, review.getScore2());
		score3.putData(put, family, review.getScore3());
	}
	
	public static void putMean(Put put, String name, byte[] family, float value) {
		if (name.equals("star")) {
			star.putMean(put, family, value);
		} else if (name.equals("score1")) {
			score1.putMean(put, family, value);
		} else if (name.equals("score2")) {
			score2.putMean(put, family, value);
		} else if (name.equals("score3")) {
			score3.putMean(put, family, value);
		} else {
			throw new IllegalArgumentException("putMean: " + name + "  not found!");
		}
	}
	
	public static void putSD(Put put, String name, byte[] family, float value) {
		if (name.equals("star")) {
			star.putSD(put, family, value);
		} else if (name.equals("score1")) {
			score1.putSD(put, family, value);
		} else if (name.equals("score2")) {
			score2.putSD(put, family, value);
		} else if (name.equals("score3")) {
			score3.putSD(put, family, value);
		} else {
			throw new IllegalArgumentException("putSD: " + name + "  not found!");
		}
	}
	
	public static byte[] getDataCol(String name) {
		if (name.equals("star")) {
			return star.getDataCol();
		} else if (name.equals("score1")) {
			return score1.getDataCol();
		} else if (name.equals("score2")) {
			return score2.getDataCol();
		} else if (name.equals("score3")) {
			return score3.getDataCol();
		} else {
			throw new IllegalArgumentException("getDataCol: " + name + "  not found!");
		}
	}
	
	public static byte[] getMeanCol(String name) {
		if (name.equals("star")) {
			return star.getMeanCol();
		} else if (name.equals("score1")) {
			return score1.getMeanCol();
		} else if (name.equals("score2")) {
			return score2.getMeanCol();
		} else if (name.equals("score3")) {
			return score3.getMeanCol();
		} else {
			throw new IllegalArgumentException("getMeanCol: " + name + " not found!");
		}
	}
	
	public static byte[] getSdCol(String name) {
		if (name.equals("star")) {
			return star.getSdCol();
		} else if (name.equals("score1")) {
			return score1.getSdCol();
		} else if (name.equals("score2")) {
			return score2.getSdCol();
		} else if (name.equals("score3")) {
			return score3.getSdCol();
		} else {
			throw new IllegalArgumentException("getSdCol: " + name + " not found!");
		}
	}
	
	public static String[] getAllScore() {
		return new String[] {
			star.getScoreName(),
			score1.getScoreName(),
			score2.getScoreName(),
			score3.getScoreName()
		};
	}
}
