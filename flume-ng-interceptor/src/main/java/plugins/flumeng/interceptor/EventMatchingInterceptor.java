package plugins.flumeng.interceptor;

import java.util.List;
import java.util.Set;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.apache.flume.interceptor.StaticInterceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * @author lichao
 *
 */
public class EventMatchingInterceptor implements Interceptor {
	private static final Logger logger = LoggerFactory.getLogger(StaticInterceptor.class);

	private final List<Set<String>> groups;

	private EventMatchingInterceptor(List<Set<String>> groups) {
		this.groups = groups;
	}

	@Override
	public void initialize() {
		// no-op
	}

	@Override
	/**
	 * Returns the event if it passes the regular expression filter and null
	 * otherwise.
	 */
	public Event intercept(Event event) {

		if (groups != null && groups.size() > 0) {
			String body = new String(event.getBody());
			for (Set<String> group : groups) {
				if (contains(body, group))
					return event;
			}
		}
		return null;
	}

	private boolean contains(String body, Set<String> group) {
		for (String item : group) {
			if (!body.contains(item))
				return false;
		}
		return true;
	}

	/**
	 * Returns the set of events which pass filters, according to
	 * {@link #intercept(Event)}.
	 * 
	 * @param events
	 * @return
	 */
	@Override
	public List<Event> intercept(List<Event> events) {
		List<Event> out = Lists.newArrayList();
		for (Event event : events) {
			Event outEvent = intercept(event);
			if (outEvent != null) {
				out.add(outEvent);
			}
		}
		return out;
	}

	@Override
	public void close() {
		// no-op
	}

	/**
	 * Builder which builds new instance of the StaticInterceptor.
	 */
	public static class Builder implements Interceptor.Builder {

		private List<Set<String>> list;
		private String regex;

		@Override
		public void configure(Context context) {
			regex = context.getString(Constants.REGEX, Constants.DEFAULT_REGEX);
			if (regex != null) {
				list = Lists.newArrayList();
				String[] groups = regex.split(";");
				for (String group : groups) {
					String[] items = group.split("&");
					Set<String> set = Sets.newHashSet(items);
					list.add(set);
				}
			} else {
				list = null;
			}

		}

		@Override
		public Interceptor build() {
			logger.info(String.format("Creating LogMatchingInterceptor: regex=(%s)", regex));
			return new EventMatchingInterceptor(list);
		}
	}

	public static class Constants {

		public static final String REGEX = "regex";
		public static final String DEFAULT_REGEX = null;
	}
}
