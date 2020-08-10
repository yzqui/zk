package com.utils.zk;

import lombok.extern.slf4j.Slf4j;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.shaded.com.google.common.collect.Maps;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
public class ZkUtil {

	private String ZK_PATH;

	private volatile Map<String, String> configs = Maps.newHashMap();

	private CuratorFramework client;

	private ExecutorService executor = Executors.newFixedThreadPool(2);

	public ZkUtil(String addr, String path) {
		RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
		this.client = CuratorFrameworkFactory.builder()
				.connectString(addr)
				.sessionTimeoutMs(50000)
				.connectionTimeoutMs(50000)
				.retryPolicy(retryPolicy)
				.build();
		this.ZK_PATH = path;
		this.client.start();
	}

	/**
	 * 自动更新配置 Bean
	 *
	 * @param configBean 配置 Bean
	 */
	public void autoUpdateConfigs(Map<String, String> configBean) {
		try {
			PathChildrenCache pathChildrenCache;
			Stat stat = client.checkExists().forPath(ZK_PATH);
			if (null == stat) {
				client.create().withMode(CreateMode.PERSISTENT).forPath(ZK_PATH);
			}
			// 获取所有的 key,用来判定新建的条件
			List<String> keys = getAllConfigNames();
			pathChildrenCache = new PathChildrenCache(client, ZK_PATH, true);
			pathChildrenCache.getListenable().addListener((curatorFramework, event) -> {
				String path = event.getData().getPath();
				String key = path.substring(path.lastIndexOf("/") + 1);
				String value = null != event.getData() ? new String(event.getData().getData(), "UTF-8") : "";
				if (PathChildrenCacheEvent.Type.CHILD_ADDED == event.getType() && !keys.contains(event.getData().getPath())) {
					// 添加
					configBean.put(key, value);
				} else if (PathChildrenCacheEvent.Type.CHILD_UPDATED == event.getType()) {
					// 更新
					configBean.put(key, value);
				} else if (PathChildrenCacheEvent.Type.CHILD_REMOVED == event.getType()) {
					// 删除
					configBean.remove(key);
				}
			}, executor);
			pathChildrenCache.start();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * 获取所有的配置名称
	 */
	private List<String> getAllConfigNames() throws Exception {
		List<String> keys = new ArrayList<>();
		List<String> childPaths = client.getChildren().forPath(ZK_PATH);
		if (null != childPaths && !childPaths.isEmpty()) {
			for (String childPath : childPaths) {
				keys.add(concatKey(childPath));
			}
		}
		return keys;
	}

	/**
	 * 插入配置
	 *
	 * @param key   配置名称
	 * @param value 配置值
	 */
	public void insertConfig(String key, String value) throws Exception {
		if (!checkChildPathExists(key)) {
			client.create().withMode(CreateMode.PERSISTENT).forPath(concatKey(key), value.getBytes("UTF-8"));
		}
	}

	/**
	 * 删除配置
	 *
	 * @param key 配置名称
	 */
	public void deleteConfig(String key) throws Exception {
		if (checkChildPathExists(key)) {
			client.delete().forPath(concatKey(key));
		} else {
			log.warn("删除 {} 失败!", key);
		}
	}

	/**
	 * 更新配置
	 *
	 * @param key   配置名称
	 * @param value 配置新的值
	 */
	public void updateConfig(String key, String value) throws Exception {
		if (checkChildPathExists(key)) {
			client.setData().forPath(concatKey(key), value.getBytes("UTF-8"));
		} else {
			log.warn("更新配置 {} 失败!", key);
		}
	}

	private String concatKey(String key) {
		return ZK_PATH.concat("/").concat(key);
	}

	private boolean checkChildPathExists(String key) throws Exception {
		Stat stat = client.checkExists().forPath(concatKey(key));
		if (null != stat) {
			log.warn("配置 {} 不存在!", key);
			return true;
		}
		return false;
	}

	/**
	 * 获取所有的配置
	 */
	public Map<String, String> getAllConfigs() throws Exception {
		if (!configs.isEmpty()) {
			return configs;
		}
		List<String> childPaths = client.getChildren().forPath(ZK_PATH);
		Map<String, String> configs = Maps.newHashMap();
		if (null != childPaths && !childPaths.isEmpty()) {
			for (String childPath : childPaths) {
				String key = childPath.substring(childPath.indexOf("/") + 1);
				String value = new String(client.getData().forPath(concatKey(childPath)), "UTF-8");
				configs.put(key, value);
			}
		}
		return configs;
	}
}
