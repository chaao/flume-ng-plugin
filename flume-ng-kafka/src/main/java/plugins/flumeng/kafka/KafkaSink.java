package plugins.flumeng.kafka;

import java.util.List;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.sink.AbstractSink;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

/**
 * kafka sink.
 */
public class KafkaSink extends AbstractSink implements Configurable {
	// - [ constant fields ] ----------------------------------------

	/**
	 * The constant logger.
	 */
	private static final Logger log = LoggerFactory.getLogger(KafkaSink.class);

	private SinkCounter sinkCounter;

	/**
	 * The Parameters.
	 */
	private Properties props;
	/**
	 * The Producer.
	 */
	private KafkaProducer<String, String> producer;
	/**
	 * The Context.
	 */
	private Context context;

	private String topic;

	/**
	 * Configure void.
	 * 
	 * @param context
	 *            the context
	 */
	@Override
	public void configure(Context context) {
		this.context = context;

		topic = Preconditions.checkNotNull((String) context.getString(Constans.CUSTOME_TOPIC_KEY_NAME), "custom.topic.name is required");
		ImmutableMap<String, String> props = context.getParameters();

		this.props = new Properties();
		for (String key : props.keySet()) {
			String value = props.get(key);
			this.props.put(key, value);
		}

		if (sinkCounter == null) {
			sinkCounter = new SinkCounter(getName());
		}
	}

	/**
	 * Start void.
	 */
	@Override
	public synchronized void start() {
		super.start();
		sinkCounter.start();
		this.producer = new KafkaProducer<String, String>(props);
		List<PartitionInfo> partitionInfos = producer.partitionsFor(topic);
		for (PartitionInfo partitionInfo : partitionInfos) {
			log.info("Connection kafka {}", partitionInfo.toString());
		}
	}

	/**
	 * Process status.
	 * 
	 * @return the status
	 * @throws EventDeliveryException
	 *             the event delivery exception
	 */
	@Override
	public Status process() throws EventDeliveryException {
		Status status = Status.READY;

		// Start transaction
		Channel channel = getChannel();
		Transaction txn = channel.getTransaction();
		txn.begin();
		try {
			// This try clause includes whatever Channel operations you want to
			// do
			Event event = channel.take();
			if (event != null) {
				String partitionKey = (String) props.get(Constans.PARTITION_KEY_NAME);
				String encoding = StringUtils.defaultIfEmpty((String) this.props.get(Constans.ENCODING_KEY_NAME), Constans.DEFAULT_ENCODING);

				String eventData = new String(event.getBody(), encoding);

				ProducerRecord<String, String> data;

				// if partition key does'nt exist
				if (StringUtils.isEmpty(partitionKey)) {
					data = new ProducerRecord<String, String>(topic, eventData);
				} else {
					data = new ProducerRecord<String, String>(topic, partitionKey, eventData);
				}

				producer.send(data);
				sinkCounter.incrementEventDrainSuccessCount();
			}
			txn.commit();
		} catch (Exception e) {
			// 不回滚
			// txn.rollback();
			// status = Status.BACKOFF;
			
			txn.commit();
			sinkCounter.incrementConnectionFailedCount();
			log.error("", e);
		} finally {
			txn.close();
		}
		return status;
	}

	/**
	 * Stop void.
	 */
	@Override
	public void stop() {
		super.stop();
		producer.close();
		sinkCounter.stop();
	}
}
