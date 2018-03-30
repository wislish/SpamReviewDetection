package com.brofan.service.classifier.sgd;

import java.io.FileWriter;
import java.io.IOException;

import com.brofan.service.classifier.sgd.entity.UserFeatures;

public class OutputUserFeature {
	
	private UserHelper helper = new UserHelper();
	
	public void output() throws IOException {
		
		FileWriter wr = new FileWriter("/home/brofan/data/user.csv");
		
		String spliter = ",";
		wr.write("RD" + spliter + "C" + spliter + "ETF" + spliter + "SR" + spliter + "inno\n");
		
		while (helper.hasNext()) {
			UserFeatures uf = helper.getUserFeatures();
			wr.write(Float.toString(uf.getRD()) + spliter);
			wr.write(Float.toString(uf.getC()) + spliter);
			wr.write(Float.toString(uf.getETF()) + spliter);
			wr.write(Integer.toString(uf.isSR()? 1:0) + spliter);
			wr.write(Integer.toString(uf.isSpam()) + "\n");
		}
		
		wr.close();
	}
	
	public static void main(String[] args) throws IOException {
		new OutputUserFeature().output();
	}
}
