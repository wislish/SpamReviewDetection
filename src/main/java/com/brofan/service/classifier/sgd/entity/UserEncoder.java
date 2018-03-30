package com.brofan.service.classifier.sgd.entity;

import org.apache.mahout.math.Vector;
import org.apache.mahout.vectorizer.encoders.ContinuousValueEncoder;

import com.brofan.service.classifier.sgd.util.CategoryFeatureEncoder;

public class UserEncoder {
	ContinuousValueEncoder rdEncoder = new ContinuousValueEncoder("rd");
	ContinuousValueEncoder cEncoder = new ContinuousValueEncoder("c");
	ContinuousValueEncoder etfEncoder = new ContinuousValueEncoder("etf");
	CategoryFeatureEncoder srEncoder = new CategoryFeatureEncoder("sr");
	
	public void addToVector(UserFeatures uf, Vector vector) {
		rdEncoder.addToVector((byte[]) null, uf.getRD(), vector);
		cEncoder.addToVector((byte[]) null, uf.getC(), vector);
		etfEncoder.addToVector((byte[]) null, uf.getETF(), vector);
		srEncoder.addToVector(uf.isSR()? 1 : 0, vector);
	}
}
