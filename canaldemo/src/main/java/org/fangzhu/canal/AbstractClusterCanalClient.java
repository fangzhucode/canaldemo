package org.fangzhu.canal;

import java.util.List;

import org.fangzhu.constant.CanalClientConf;
import org.fangzhu.constant.CanalRecord;
import org.fangzhu.util.AnalysisBinLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.util.Assert;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.protocol.Message;

/**
 * canal集群测试具体执行方法
 * 
 * @author fangzhu
 * @date 2016-3-1
 */
public class AbstractClusterCanalClient {

	protected final static Logger logger = LoggerFactory
			.getLogger(AbstractClusterCanalClient.class);
	protected volatile boolean running = false;
	protected Thread.UncaughtExceptionHandler handler = new Thread.UncaughtExceptionHandler() {
		public void uncaughtException(Thread t, Throwable e) {
			logger.error("parse events has an error", e);
		}
	};
	protected Thread thread = null;
	protected CanalConnector connector;
	protected String destination;
	protected long sleepTime = 1000 * 5;

	public AbstractClusterCanalClient() {
	}

	public AbstractClusterCanalClient(String destination) {
		this(destination, null);
	}

	public AbstractClusterCanalClient(String destination,
			CanalConnector connector) {
		this.destination = destination;
		this.connector = connector;
	}

	protected void start() {
		Assert.notNull(connector, "connector is null");
		thread = new Thread(new Runnable() {
			public void run() {
				process();
			}
		});

		thread.setUncaughtExceptionHandler(handler);
		thread.start();
		running = true;
	}

	protected void stop() {
		if (!running) {
			return;
		}
		running = false;
		if (thread != null) {
			try {
				thread.join();
			} catch (InterruptedException e) {
				// ignore
			}
		}
		MDC.remove("destination");
	}

	protected void process() {
		while (running) {
			try {
				MDC.put("destination", destination);
				connector.connect();
				connector.subscribe();
				while (running) {
					// Message message =
					// connector.get(CanalClientConf.BATCH_SIZE);// 获取指定数量的数据
					Message message = connector
							.getWithoutAck(CanalClientConf.BATCH_SIZE);// 获取指定数量的数据
					long batchId = message.getId();
					int size = message.getEntries().size();
					if (batchId == -1 || size == 0) {
						logger.info("message size=" + size + " batchid="
								+ batchId + " sleep " + sleepTime + "ms");
						Thread.sleep(sleepTime);
					} else {
						List<CanalRecord> results = AnalysisBinLog
								.analysisBinLog(message.getEntries());
						if (results.size() > 0) {
							// 按条拆分结果 便于后面处理 如反序列化等
							for (CanalRecord canalRecord : results) {
								String topic = canalRecord.getDatabase() + "."
										+ canalRecord.getTable();
								logger.info("result:"
										+ JSONObject.toJSONString(canalRecord));
							}
						} else {
							Thread.sleep(sleepTime);
							logger.info("result size is 0 sleep " + sleepTime);
						}
					}
					connector.ack(batchId); // 提交确认
					// connector.rollback(batchId); // 处理失败, 回滚数据
				}
			} catch (Exception e) {
				logger.error("process error!", e);
			} finally {
				connector.disconnect();
				MDC.remove("destination");
			}
		}
	}

	public void setConnector(CanalConnector connector) {
		this.connector = connector;
	}
}