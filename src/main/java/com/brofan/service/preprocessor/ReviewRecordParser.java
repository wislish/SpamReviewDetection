package com.brofan.service.preprocessor;

import java.sql.Timestamp;
import java.util.NoSuchElementException;
import java.util.StringTokenizer;

import com.brofan.table.entity.type.LogReasonType;
import org.apache.hadoop.io.Text;

import com.brofan.service.preprocessor.entity.Review;
import com.brofan.service.preprocessor.entity.ReviewType;


public class ReviewRecordParser {
	private Review review;
	private ReviewType status;
	
	public ReviewRecordParser() {
		review = new Review();
		status = ReviewType.UNKNOWN;
	}
	
	public void parse(Text line) {
		parse(line.toString(), "\t");
	}

	/*
	public void parse(String line, String separator) {
		
		// TODO： StringTokenizer
		try {
			int p = 0;
			String[] fields = line.split(separator);
			review.setId( fields[p++] );
			StringBuffer body = new StringBuffer(fields[p++]);
			
			while (LogReasonType.isLogReason(fields[p]) == false) {
				body.append(fields[p++]);
			}
			review.setBody( body.toString() );
			
			LogReasonType lr = LogReasonType.valueOf( Integer.parseInt(fields[p++]) );
			review.setLogreason(lr);
			
			review.setUpdatetime( Timestamp.valueOf(fields[p++]) );
			
			review.setUserid( fields[p++] );
			review.setShopid( fields[p++] );
			review.setStar( Integer.parseInt(fields[p++]) / 10 );
			review.setScore1( Integer.parseInt(fields[p++]) );
			review.setScore2( Integer.parseInt(fields[p++]) );
			review.setScore3( Integer.parseInt(fields[p++]) );
			
			status = ReviewType.VALID;
		} catch (java.lang.IllegalArgumentException e) {
			status = ReviewType.MALFORMED;
		} catch (java.lang.ArrayIndexOutOfBoundsException ae) {
			status = ReviewType.MISSING;
		}
	}
	*/
	public void parse(String line, String separator) {

		if (separator == null) return;

		try {
			StringTokenizer st = new StringTokenizer(line, separator);

			review.setId(st.nextToken());

			StringBuffer body = new StringBuffer();
			String part = st.nextToken();
			while (LogReasonType.isLogReason(part) == false) {
				body.append(part);
				part = st.nextToken();
			};
			review.setBody(body.toString());

			LogReasonType lr = LogReasonType.valueOf( Integer.parseInt(part) );
			review.setLogreason(lr);

			review.setUpdatetime(Timestamp.valueOf(st.nextToken()));

			review.setUserid(st.nextToken());
			review.setShopid(st.nextToken());

			int star = Integer.parseInt(st.nextToken()) / 10;
			if (star < 1 || star > 5) throw new IllegalArgumentException("Illegal star value!");
			review.setStar(star);

			int score = Integer.parseInt(st.nextToken());
			if (score < -1 || score > 4) throw new IllegalArgumentException("Illegal score1 value!");
			review.setScore1( score );

			score = Integer.parseInt(st.nextToken());
			if (score < -1 || score > 4) throw new IllegalArgumentException("Illegal score2 value!");
			review.setScore2( score );

			score = Integer.parseInt(st.nextToken());
			if (score < -1 || score > 4) throw new IllegalArgumentException("Illegal score3 value!");
			review.setScore3( score );

			status = ReviewType.VALID;
		} catch (java.lang.IllegalArgumentException e) {
			status = ReviewType.MALFORMED;
		} catch (NoSuchElementException ne) {
			status = ReviewType.MISSING;
		}
	}
	
	public boolean isValidRecord() {
		return (status == ReviewType.VALID);
	}
	
	public boolean isMalformedReview() {
		return (status == ReviewType.MALFORMED);
	}
	
	public boolean isMissingReview() {
		return (status == ReviewType.MISSING);
	}
	
	public Review getReview() {
		return review;
	}
}
