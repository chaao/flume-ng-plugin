package plugins.flumeng.hdfs.source;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.instrumentation.SourceCounter;
import org.apache.flume.source.AbstractSource;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/**
 * @author chao.li
 *
 */
public class HDFSSource extends AbstractSource implements Configurable, EventDrivenSource {

	private static final Logger logger = LoggerFactory.getLogger("HDFSSource");

	static Gson gson = new GsonBuilder().enableComplexMapKeySerialization().serializeNulls().create();
	private Context context;
	private SourceCounter sourceCounter;
	private ExecutorService singleExecutor;
	private ExecutorService taskExecutor;
	private Pattern pattern;

	private Configuration conf;
	private URI uri;

	private String hdfsUrl;
	private String filePattern;
	private int threadNum;

	@Override
	public void configure(Context context) {
		this.context = context;

		this.hdfsUrl = Preconditions.checkNotNull(context.getString(Constans.HDFS_URL), "hdfs.url is required");
		this.filePattern = Preconditions.checkNotNull(context.getString(Constans.FILE_PATTERN),
				"file.pattern is required");
		String threadNumStr = Preconditions.checkNotNull(context.getString(Constans.THREAD_NUM),
				"thread.num is required");
		this.threadNum = NumberUtils.toInt(threadNumStr, 5);

		if (this.sourceCounter == null) {
			this.sourceCounter = new SourceCounter(getName());
		}

		this.singleExecutor = Executors.newSingleThreadExecutor();
		this.taskExecutor = Executors.newFixedThreadPool(this.threadNum);
		pattern = Pattern.compile("\\((\\d*),Set\\({2}(.*)\\){3}");

		this.conf = new Configuration();
		this.uri = URI.create(this.hdfsUrl);

		logger.info("configure success! hdfsUrl:{}, filePattern:{}, threadNum:{}",
				new Object[] { this.hdfsUrl, filePattern, threadNum });
	}

	@Override
	public synchronized void start() {
		sourceCounter.start();
		super.start();

		singleExecutor.submit(new Runner());

		logger.info("HDFS source started with url:{}", hdfsUrl);
	}

	@Override
	public synchronized void stop() {
		sourceCounter.stop();
		taskExecutor.shutdownNow();
		singleExecutor.shutdownNow();
		super.stop();
		logger.info("HDFS source stoped!");
	}

	private class Runner implements Runnable {

		@Override
		public void run() {

			while (true) {

				String filePath = String.format(filePattern, DateUtils.truncateToTenMinute());
				process(filePath);
				try {
					TimeUnit.MINUTES.sleep(10);
				} catch (InterruptedException e) {
					logger.error("", e);
				}
			}

		}

		private void process(String filePath) {

			Stopwatch watch = new Stopwatch();
			watch.start();

			logger.info("========== start from === {}", filePath);

			final AtomicLong totalCount = new AtomicLong(0);
			FileSystem fs = null;
			try {
				fs = getFs();
				Path path = new Path(filePath);
				boolean exist = fs.exists(path);
				boolean directory = fs.isDirectory(path);

				if (exist && directory) {
					FileStatus[] files = fs.listStatus(path);
					List<Path> subPaths = Lists.newArrayList();
					for (FileStatus file : files) {
						if (file.getPath().getName().startsWith("part-")) {
							subPaths.add(file.getPath());
						}
					}

					CountDownLatch cdl = new CountDownLatch(subPaths.size());

					for (Path subPath : subPaths) {
						taskExecutor.submit(new Runnable() {
							@Override
							public void run() {

								InputStream in = null;
								InputStreamReader reader = null;
								BufferedReader bufferReader = null;
								long subCount = 0;
								try {
									in = getFs().open(subPath);
									reader = new InputStreamReader(in, Constans.DEFAULT_ENCODING);
									bufferReader = new BufferedReader(reader);
									String line = "";
									while ((line = bufferReader.readLine()) != null) {

										String gmpValue = formatData(line);
										if (StringUtils.isNotBlank(gmpValue)) {
											Event e = EventBuilder.withBody(gmpValue, Charsets.UTF_8);
											getChannelProcessor().processEvent(e);
											totalCount.incrementAndGet();
											subCount++;
										}
									}
								} catch (Exception e) {
									logger.error("read error from " + subPath.getName(), e);
								} finally {
									IOUtils.closeStream(bufferReader);
									IOUtils.closeStream(reader);
									IOUtils.closeStream(in);
									logger.info("subPath: obtian {} rows from {}", subCount, subPath.getName());
									cdl.countDown();
								}
							}
						});
					}

					cdl.await();
					logger.info("total: obtian {} rows from {}", totalCount.get(), path.getName());

				} else {
					logger.error("error filePath({})!!! exist:{}, directory:{}",
							new Object[] { filePath, exist, directory });
					return;
				}

			} catch (Throwable t) {
				logger.error(String.format("read error from %s", filePath), t);
			} finally {
				if (fs != null) {
					try {
						fs.close();
					} catch (IOException e) {
						logger.error("", e);
					}
				}
			}
			watch.stop();
			logger.info("========== stop ==== {} === time:{}", filePath, watch.toString());

		}

	}

	private String formatData(String srcdata) {
		// String srcdata = "(38491483,Set((wifiyaoshi,2.0,83.0,0.024096385),
		// (coolpad,5.0,51.0,0.09803922), (-1,11.0,263.0,0.041825093),
		// (emui,3.0,86.0,0.034883723)))";
		String data = srcdata.replaceAll("\\s*", "");

		Matcher matcher = pattern.matcher(data);
		if (matcher.matches()) {
			List<Map<String, ChannelValue>> channels = Lists.newArrayList();
			String gmpList[] = matcher.group(2).split("\\),\\(");
			for (String item : gmpList) {
				String[] gmpValueArray = item.split(",");
				ChannelValue channelValue = new ChannelValue();

				channelValue.setClick(NumberUtils.toDouble(gmpValueArray[1]));
				channelValue.setImpression(NumberUtils.toDouble(gmpValueArray[2]));
				channelValue.setCtr(NumberUtils.toDouble(gmpValueArray[3]));

				Map<String, ChannelValue> channel = Maps.newHashMap();
				channel.put(gmpValueArray[0], channelValue);
				channels.add(channel);
			}

			Gson gson = new GsonBuilder().enableComplexMapKeySerialization().serializeNulls().create();
			GmpModel gmpModel = new GmpModel();
			gmpModel.setId(matcher.group(1));
			gmpModel.setChannels(gson.toJson(channels));

			return gson.toJson(gmpModel);
		} else {
			logger.error("match data error! srcdata:{}", srcdata);
			return null;
		}

	}

	private FileSystem getFs() throws IOException {
		return FileSystem.get(uri, this.conf);
	}

}
