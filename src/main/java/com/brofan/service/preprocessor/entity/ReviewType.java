package com.brofan.service.preprocessor.entity;

public enum ReviewType {
	VALID,			// vaild review
	MISSING,			// some parts of this review are missing
	MALFORMED,		// this review is malformed
	UNKNOWN			// not processed
}
