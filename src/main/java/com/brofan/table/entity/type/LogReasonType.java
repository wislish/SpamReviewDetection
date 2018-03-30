package com.brofan.table.entity.type;

public enum LogReasonType {
	NOTFOUND(-1),	// 暂未发现违规
	HYPED(5),		// 炒作
	INEXP(6),		// 非亲身体验
	SPAM(16),		// 灌水
	AD(17),			// 广告
	ABUSE(18);		// 不当语言
	
	private int value;
	
	LogReasonType(int value) {
		this.value = value;
	}
	public int getValue() {
		return value;
	}
	
	public static LogReasonType valueOf(int value) {
		switch (value) {
		case -1:
			return NOTFOUND;
		case 5:
			return HYPED;
		case 6:
			return INEXP;
		case 16:
			return SPAM;
		case 17:
			return AD;
		case 18:
			return ABUSE;
		default:
			throw new java.lang.IllegalArgumentException();
		}
	}
	
	public static boolean isLogReason(String s) {
		try {
			int i = Integer.parseInt(s);
			valueOf(i);
			return true;
		} catch (java.lang.NumberFormatException ne) {
			return false;
		} catch (java.lang.IllegalArgumentException ie) {
			return false;
		}
	}
}
