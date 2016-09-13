package plugins.flumeng.hdfs.source;

import java.util.Calendar;

import org.apache.commons.lang.time.FastDateFormat;

/**
 * @author chao.li
 *
 */
public class DateUtils {

	public static long truncateToTenMinute() {

		Calendar val = Calendar.getInstance();

		long time = val.getTimeInMillis();

		int millisecs = val.get(Calendar.MILLISECOND);
		time = time - millisecs;

		int seconds = val.get(Calendar.SECOND);
		time = time - (seconds * 1000L);

		int minutes = val.get(Calendar.MINUTE);
		int span = minutes % 10;
		if (span > 0) {
			time = time - (span * 60000L);
		}

		return time;
	}

	public static void main(String[] args) {

		FastDateFormat dateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss-SS");
		System.out.println(dateFormat.format(truncateToTenMinute()));
	}
}
