package plugins.flumeng.redis.sink;

import java.io.IOException;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

/**
 * @author chao.li
 *
 */
public class RedisSink extends AbstractSink implements Configurable {

	private static final Logger logger = LoggerFactory.getLogger("RedisSink");
	static Gson gson = new GsonBuilder().enableComplexMapKeySerialization().serializeNulls().create();

	private SinkCounter sinkCounter;
	private JedisCluster jedisCluster;

	private String srcAddress;
	private String hashKey;
	private int timeout;

	@Override
	public Status process() throws EventDeliveryException {

		Status result = Status.READY;
		// Start transaction
		Channel channel = getChannel();
		Transaction transaction = channel.getTransaction();
		Event event = null;

		try {
			transaction.begin();
			event = channel.take();

			if (event != null) {
				String eventData = new String(event.getBody(), Constans.DEFAULT_ENCODING);
				GmpModel gmpModel = gson.fromJson(eventData, GmpModel.class);

				if (gmpModel == null || StringUtils.isBlank(gmpModel.getId())
						|| StringUtils.isBlank(gmpModel.getChannels())) {
					throw new Exception("parse json error! eventData:" + eventData);
				}

				jedisCluster.hset(hashKey, gmpModel.getId(), gmpModel.getChannels());
				sinkCounter.incrementEventDrainSuccessCount();

			} else {
				result = Status.BACKOFF;
			}
			transaction.commit();
		} catch (Exception e) {
			transaction.rollback();
			sinkCounter.incrementConnectionFailedCount();
			logger.error("", e);
		} finally {
			transaction.close();
		}

		return result;
	}

	@Override
	public void configure(Context context) {

		this.srcAddress = Preconditions.checkNotNull(context.getString(Constans.REDIS_ADDRESS),
				"redis.address is required");
		this.hashKey = Preconditions.checkNotNull(context.getString(Constans.HASH_KEY), "hash.key is required");
		String timeoutStr = Preconditions.checkNotNull(context.getString(Constans.REDIS_TIMEOUT),
				"redis.timeout is required");
		this.timeout = NumberUtils.toInt(timeoutStr, 3000);

		Set<HostAndPort> nodes = parseAddress(this.srcAddress);
		jedisCluster = new JedisCluster(nodes, this.timeout);

		if (sinkCounter == null) {
			sinkCounter = new SinkCounter(getName());
		}
		logger.info("configure success! redis.address:{}, hash.key:{}, timeout:{}",
				new Object[] { this.srcAddress, this.hashKey, this.timeout });
	}

	@Override
	public synchronized void start() {
		super.start();
		sinkCounter.start();
		logger.info("redis sink started!");
	}

	@Override
	public void stop() {
		super.stop();
		try {
			jedisCluster.close();
		} catch (IOException e) {
			logger.error("", e);
		}
		sinkCounter.stop();
		logger.info("redis sink stoped!");
	}

	private Set<HostAndPort> parseAddress(String str) {

		Set<HostAndPort> nodes = Sets.newHashSet();
		String addressStr = str.replaceAll("\\s*", "");
		String[] addressArray = addressStr.split(",");
		for (String address : addressArray) {
			String[] ipAndPort = address.split(":");
			HostAndPort host = new HostAndPort(ipAndPort[0], Integer.parseInt(ipAndPort[1]));
			nodes.add(host);
		}
		return nodes;
	}

}
