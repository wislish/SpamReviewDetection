package com.brofan.service.classifier.sgd.entity;

import org.apache.mahout.math.Vector;
import org.apache.mahout.vectorizer.encoders.ContinuousValueEncoder;

public class ShopEncoder {
	ContinuousValueEncoder srEncoder = new ContinuousValueEncoder("sr");
	ContinuousValueEncoder bstEncoder = new ContinuousValueEncoder("bst");
	ContinuousValueEncoder dcvEncoder = new ContinuousValueEncoder("dcv");
	ContinuousValueEncoder rcvEncoder = new ContinuousValueEncoder("rcv");
	ContinuousValueEncoder starEncoder = new ContinuousValueEncoder("star");
	ContinuousValueEncoder score1Encoder = new ContinuousValueEncoder("score1");
	ContinuousValueEncoder score2Encoder = new ContinuousValueEncoder("score2");
	ContinuousValueEncoder score3Encoder = new ContinuousValueEncoder("score3");
	
	public void addToVector(ShopFeatures sf, Vector vector) {
		srEncoder.addToVector((byte[])null, sf.getSR(), vector);
		bstEncoder.addToVector((byte[])null, sf.getBST(), vector);
		dcvEncoder.addToVector((byte[])null, sf.getDCV(), vector);
		rcvEncoder.addToVector((byte[])null, sf.getRCV(), vector);
		starEncoder.addToVector((byte[])null, sf.getStar(), vector);
		score1Encoder.addToVector((byte[])null, sf.getScore1(), vector);
		score2Encoder.addToVector((byte[])null, sf.getScore2(), vector);
		score3Encoder.addToVector((byte[])null, sf.getScore3(), vector);
	}
}
