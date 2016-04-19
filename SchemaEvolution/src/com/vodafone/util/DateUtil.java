package com.vodafone.util;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class DateUtil {

	public static String convertTimeIntoFormat(long intakeTime,String projectTimeFormat) {

		Calendar c = Calendar.getInstance();
		c.setTimeInMillis(intakeTime);
		Date date = c.getTime();
		SimpleDateFormat sdf = new SimpleDateFormat(projectTimeFormat);
		String stringDate = sdf.format(date);
		return stringDate;
	}
	
	public static void main(String[] args) {
		
		System.out.println(DateUtil.convertTimeIntoFormat(System.currentTimeMillis(), "yyyyMMdd HH:mm:ss"));
	}

}
