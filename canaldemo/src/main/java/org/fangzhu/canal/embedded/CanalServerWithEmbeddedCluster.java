package org.fangzhu.canal.embedded;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.I0Itec.zkclient.ZkClient;
import org.fangzhu.constant.CanalClientConf;
import org.fangzhu.constant.CanalRecord;
import org.fangzhu.util.AnalysisBinLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.instance.core.CanalInstance;
import com.alibaba.otter.canal.instance.core.CanalInstanceGenerator;
import com.alibaba.otter.canal.instance.manager.CanalInstanceWithManager;
import com.alibaba.otter.canal.instance.manager.model.Canal;
import com.alibaba.otter.canal.instance.manager.model.CanalParameter;
import com.alibaba.otter.canal.instance.manager.model.CanalParameter.HAMode;
import com.alibaba.otter.canal.instance.manager.model.CanalParameter.IndexMode;
import com.alibaba.otter.canal.instance.manager.model.CanalParameter.MetaMode;
import com.alibaba.otter.canal.instance.manager.model.CanalParameter.SourcingType;
import com.alibaba.otter.canal.instance.manager.model.CanalParameter.StorageMode;
import com.alibaba.otter.canal.protocol.ClientIdentity;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.server.embedded.CanalServerWithEmbedded;

/**
 * canal内嵌模式 demo
 * 
 * @author fangzhu
 * @date 2016-3-22
 */
public class CanalServerWithEmbeddedCluster {
	protected final static Logger logger = LoggerFactory
			.getLogger(CanalServerWithEmbeddedCluster.class);
	protected static long sleepTime = 1000 * 5;
	protected static final String cluster1 = "192.168.1.203:2181";
	protected static final String DESTINATION = "cobarc";
	protected static final String FILTER = ".*\\..*";
	protected static final String MYSQL_ADDRESS = "192.168.1.82";
	protected static final String USERNAME = "canal";
	protected static final String PASSWORD = "0d4bd0232365bd36";
	private static CanalServerWithEmbedded server;
	private static ClientIdentity clientIdentity;

	public static void main(String[] args) {
		// ZkClient zkClient = new ZkClient(cluster1);
		// zkClient.deleteRecursive(ZookeeperPathUtils.CANAL_ROOT_NODE);
		init();
		Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(
				new Runnable() {
					@Override
					public void run() {
						getWithoutAck();
					}
				}, 0, 2, TimeUnit.SECONDS);
	}

	public static void init() {
		clientIdentity = new ClientIdentity(DESTINATION, (short) 1);
		server = new CanalServerWithEmbedded();
		;
		server.setCanalInstanceGenerator(new CanalInstanceGenerator() {
			public CanalInstance generate(String destination) {
				Canal canal = buildCanal();
				return new CanalInstanceWithManager(canal, FILTER);
			}
		});
		server.start();
		server.start(DESTINATION);
	}

	protected static Canal buildCanal() {
		Canal canal = new Canal();
		canal.setId(1L);
		canal.setName(DESTINATION);
		canal.setDesc("test");

		CanalParameter parameter = new CanalParameter();

		parameter.setZkClusters(Arrays.asList(cluster1));
		parameter.setMetaMode(MetaMode.MIXED); // 冷备，可选择混合模式
		parameter.setHaMode(HAMode.HEARTBEAT);
		parameter.setIndexMode(IndexMode.META);// 内存版store，需要选择meta做为index

		parameter.setStorageMode(StorageMode.MEMORY);
		parameter.setMemoryStorageBufferSize(32 * 1024);

		parameter.setSourcingType(SourcingType.MYSQL);
		parameter.setDbAddresses(Arrays.asList(new InetSocketAddress(
				MYSQL_ADDRESS, 3901),
				new InetSocketAddress(MYSQL_ADDRESS, 3901)));
		parameter.setDbUsername(USERNAME);
		parameter.setDbPassword(PASSWORD);
		// parameter
		// .setPositions(Arrays
		// .asList("{\"journalName\":\"mysql-bin.000001\",\"position\":6163L,\"timestamp\":1322803601000L}",
		// "{\"journalName\":\"mysql-bin.000001\",\"position\":6163L,\"timestamp\":1322803601000L}"));

		parameter.setSlaveId(1234L);

		parameter.setDefaultConnectionTimeoutInSeconds(30);
		parameter.setConnectionCharset("UTF-8");
		parameter.setConnectionCharsetNumber((byte) 33);
		parameter.setReceiveBufferSize(8 * 1024);
		parameter.setSendBufferSize(8 * 1024);

		parameter.setDetectingEnable(false);
		parameter.setDetectingIntervalInSeconds(10);
		parameter.setDetectingRetryTimes(3);
		// parameter.setDetectingSQL(DETECTING_SQL);

		canal.setCanalParameter(parameter);
		return canal;
	}

	public static void getWithoutAckBak() {
		int maxEmptyCount = 10;
		int emptyCount = 0;
		int totalCount = 0;
		server.subscribe(clientIdentity);
		while (emptyCount < maxEmptyCount) {
			Message message = server.getWithoutAck(clientIdentity, 11);
			if (CollectionUtils.isEmpty(message.getEntries())) {
				emptyCount++;
				try {
					Thread.sleep(emptyCount * 300L);
				} catch (InterruptedException e) {
				}

				System.out.println("empty count : " + emptyCount);
			} else {
				emptyCount = 0;
				totalCount += message.getEntries().size();
				server.ack(clientIdentity, message.getId());
			}
		}

		System.out.println("!!!!!! testGetWithoutAck totalCount : "
				+ totalCount);
		server.unsubscribe(clientIdentity);
	}

	public static void getWithoutAck() {
		if (server.isStart()) {
			logger.info("start server");
		}
		if (server.isStart(DESTINATION)) {
			logger.info("start server " + DESTINATION);
		}
		logger.info("=====开始解析=====");
		server.subscribe(clientIdentity);
		Message message = server
				.get(clientIdentity, CanalClientConf.BATCH_SIZE);// 获取指定数量的数据
		// Message message = server.getWithoutAck(clientIdentity,
		// CanalClientConf.BATCH_SIZE);// 获取指定数量的数据
		long batchId = message.getId();
		int size = message.getEntries().size();
		if (batchId == -1 || size == 0) {
			logger.info("message size=" + size + " batchid=" + batchId
					+ " sleep " + sleepTime + "ms");
			try {
				Thread.sleep(sleepTime);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		} else {
			List<CanalRecord> results = AnalysisBinLog.analysisBinLog(message
					.getEntries());
			if (results.size() > 0) {
				// 按条拆分结果 便于后面处理 如反序列化等
				for (CanalRecord canalRecord : results) {
					String topic = canalRecord.getDatabase() + "."
							+ canalRecord.getTable();
					logger.info("result:"
							+ JSONObject.toJSONString(canalRecord));
				}
			} else {
				try {
					Thread.sleep(sleepTime);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				logger.info("result size is 0 sleep " + sleepTime);
			}
		}
		// server.ack(clientIdentity, message.getId());
		server.unsubscribe(clientIdentity);
		logger.info("=====结束解析=====");
	}
}
