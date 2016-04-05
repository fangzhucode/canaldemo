package org.fangzhu.canal;

import org.apache.commons.lang.exception.ExceptionUtils;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;

/**
 * canal集群测试Main方法
 * 
 * @author fangzhu
 * @date 2016-3-1
 */
public class ClusterCanalClient extends AbstractClusterCanalClient {

	public ClusterCanalClient(String destination) {
		super(destination);
	}

	public static void main(String args[]) {
		// String destination = "example";
		String destination = "cobarc";

		// 基于zookeeper动态获取canal server的地址，建立链接，其中一台server发生crash，可以支持failover
		CanalConnector connector = CanalConnectors.newClusterConnector(
				"192.168.1.203:2181", destination, "", "");
		// 192.168.1.203:2181:cobarc

		final ClusterCanalClient clientTest = new ClusterCanalClient(
				destination);
		clientTest.setConnector(connector);
		clientTest.start();

		Runtime.getRuntime().addShutdownHook(new Thread() {
			public void run() {
				try {
					logger.info("## stop the canal client");
					clientTest.stop();
				} catch (Throwable e) {
					logger.warn(
							"##something goes wrong when stopping canal:\n{}",
							ExceptionUtils.getFullStackTrace(e));
				} finally {
					logger.info("## canal client is down.");
				}
			}

		});
	}
}
