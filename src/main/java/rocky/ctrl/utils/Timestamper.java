package rocky.ctrl.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/*
 * It is tested to be accurate in the millisecond order.
 */
public class Timestamper {

	public Timestamper() {
		
	}
	
	public static String getTimestamp() {
		//SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd_HHmmss");
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SS");
		String localTime = sdf.format(new Date());
		return localTime;
	}
	
	//add gap milliseconds to the ts.
	public static String incTimestamp(String ts, long gap) throws ParseException {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SS");
		Date date = sdf.parse(ts);
		long base = date.getTime();
		long advance = base + gap;
		date = new Date(advance);
		return sdf.format(date);
	}
	
	//compare ts1 and ts2 and return negative if ts1 < ts2, 0 if ts1==ts2, else return 1.
	public static int compareTimestamp(String ts1, String ts2) throws ParseException {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SS");
		Date date1 = sdf.parse(ts1);
		Date date2 = sdf.parse(ts2);
		return date1.compareTo(date2);
	}
	
	//parse the ts and return Date value for the string
	public static Date parseTimestampString(String ts) throws ParseException {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SS");
		Date date = sdf.parse(ts);
		
		return date;
	}
	
	public static String convertToString(Date date) {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SS");
		String strForm = sdf.format(date);
		return strForm;
	}
	
	public static void main(String[] args) throws Exception {
		String curr = Timestamper.getTimestamp();
		String expirationTime = Timestamper.incTimestamp(curr, 1000); 
		String beforeSleep = Timestamper.getTimestamp();
		Thread.sleep(1000);
		String afterSleep = Timestamper.getTimestamp();
		int comp1 = Timestamper.compareTimestamp(beforeSleep, expirationTime);
		if (comp1 < 0) {
			System.out.println("Before sleep, exprationTime hasn't been passed");
		} else {
			System.out.println("ERROR for Before sleep comparison. comp1=" + comp1);
		}
		int comp2 = Timestamper.compareTimestamp(afterSleep, expirationTime);
		if (comp2 > 0) {
			System.out.println("After sleep, exprationTime has been passed");
		} else {
			System.out.println("ERROR for After sleep comparison. comp2=" + comp2);
		}
		
	}
	
}

