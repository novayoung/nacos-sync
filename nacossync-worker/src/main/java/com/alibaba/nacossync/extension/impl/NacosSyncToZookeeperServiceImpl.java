/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package com.alibaba.nacossync.extension.impl;

import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.naming.NamingService;
import com.alibaba.nacos.api.naming.listener.EventListener;
import com.alibaba.nacos.api.naming.listener.NamingEvent;
import com.alibaba.nacos.api.naming.pojo.Instance;
import com.alibaba.nacos.api.naming.pojo.ListView;
import com.alibaba.nacossync.cache.SkyWalkerCacheServices;
import com.alibaba.nacossync.constant.ClusterTypeEnum;
import com.alibaba.nacossync.constant.FrameworkEnum;
import com.alibaba.nacossync.constant.MetricsStatisticsType;
import com.alibaba.nacossync.constant.SkyWalkerConstants;
import com.alibaba.nacossync.extension.SyncService;
import com.alibaba.nacossync.extension.annotation.NacosSyncService;
import com.alibaba.nacossync.extension.holder.NacosServerHolder;
import com.alibaba.nacossync.extension.holder.ZookeeperServerHolder;
import com.alibaba.nacossync.monitor.MetricsManager;
import com.alibaba.nacossync.pojo.model.TaskDO;
import com.alibaba.nacossync.util.DubboConstants;
import com.google.common.collect.Sets;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.utils.CloseableUtils;
import org.apache.zookeeper.CreateMode;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.UnsupportedEncodingException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static com.alibaba.nacossync.util.DubboConstants.ALL_SERVICE_NAME_PATTERN;
import static com.alibaba.nacossync.util.StringUtils.convertDubboFullPathForZk;
import static com.alibaba.nacossync.util.StringUtils.convertDubboProvidersPath;

/**
 * Nacos 同步 Zk 数据
 *
 * @author paderlol
 * @date 2019年01月06日, 15:08:06
 */
@Slf4j
@NacosSyncService(sourceCluster = ClusterTypeEnum.NACOS, destinationCluster = ClusterTypeEnum.ZK, framework = FrameworkEnum.DUBBO)
public class NacosSyncToZookeeperServiceImpl implements SyncService {

    @Autowired
    private MetricsManager metricsManager;

    /**
     * @description The Nacos listener map.
     */
    private final Map<String, EventListener> nacosListenerMap = new ConcurrentHashMap<>();
    /**
     * instance backup
     */
    private final Map<String, Set<String>> instanceBackupMap = new ConcurrentHashMap<>();

    /**
     * listener cache of zookeeper format: taskId -> PathChildrenCache instance
     */
    private final Map<String, PathChildrenCache> pathChildrenCacheMap = new ConcurrentHashMap<>();

    /**
     * zookeeper path for dubbo providers
     */
    final Map<String, String> monitorPath = new ConcurrentHashMap<>();
    /**
     * @description The Sky walker cache services.
     */
    final SkyWalkerCacheServices skyWalkerCacheServices;

    /**
     * @description The Nacos server holder.
     */
    protected final NacosServerHolder nacosServerHolder;

    private final ZookeeperServerHolder zookeeperServerHolder;

    private Map<String, AtomicBoolean> startedMap = new HashMap<>();

    private Map<String, Set<String>> subscribeMap = new HashMap<>();

    @Autowired
    public NacosSyncToZookeeperServiceImpl(SkyWalkerCacheServices skyWalkerCacheServices,
        NacosServerHolder nacosServerHolder, ZookeeperServerHolder zookeeperServerHolder) {
        this.skyWalkerCacheServices = skyWalkerCacheServices;
        this.nacosServerHolder = nacosServerHolder;
        this.zookeeperServerHolder = zookeeperServerHolder;
    }

    @Override
    public boolean delete(TaskDO taskDO) {
        try {
            NamingService sourceNamingService =
                nacosServerHolder.get(taskDO.getSourceClusterId(), taskDO.getNameSpace());
            EventListener eventListener = nacosListenerMap.remove(taskDO.getTaskId());
            PathChildrenCache pathChildrenCache = pathChildrenCacheMap.get(taskDO.getTaskId());
            for(String service : subscribeMap.getOrDefault(taskDO.getTaskId(), Sets.newHashSet())) {
                sourceNamingService.unsubscribe(service, eventListener);
            }
            CloseableUtils.closeQuietly(pathChildrenCache);
            Set<String> instanceUrlSet = instanceBackupMap.get(taskDO.getTaskId());
            CuratorFramework client = zookeeperServerHolder.get(taskDO.getDestClusterId(), taskDO.getGroupName());
            if (instanceUrlSet != null) {
                for (String instanceUrl : instanceUrlSet) {
                    deleteNode(client, instanceUrl);
                }
            }
            startedMap.computeIfAbsent(taskDO.getTaskId(), key -> new AtomicBoolean(false)).set(false);
        } catch (Exception e) {
            log.error("delete task from nacos to zk was failed, taskId:{}", taskDO.getTaskId(), e);
            metricsManager.recordError(MetricsStatisticsType.DELETE_ERROR);
            return false;
        }
        return true;
    }

    @Override
    public boolean sync(TaskDO taskDO) {
        try {
            startedMap.computeIfAbsent(taskDO.getTaskId(), key -> new AtomicBoolean(true)).set(true);
            NamingService sourceNamingService =
                nacosServerHolder.get(taskDO.getSourceClusterId(), taskDO.getNameSpace());
            CuratorFramework client = zookeeperServerHolder.get(taskDO.getDestClusterId(), taskDO.getGroupName());
            nacosListenerMap.putIfAbsent(taskDO.getTaskId(), event -> {
                if (!startedMap.get(taskDO.getTaskId()).get()) {
                    return;
                }
                if (event instanceof NamingEvent) {
                    try {
                        String service = getRealServiceName(((NamingEvent) event).getServiceName());
                        List<Instance> sourceInstances =  sourceNamingService.getAllInstances(service); //getAllInstance(sourceNamingService, taskDO);
                        Set<String> newInstanceUrlSet = getWaitingToAddInstance(taskDO, client, service, sourceInstances);

                        if (!newInstanceUrlSet.isEmpty()) {
                            String instanceCacheKey = taskDO.getTaskId() + "-" + service;
                            deleteInvalidInstances(instanceCacheKey, client, newInstanceUrlSet); // 获取之前的备份 删除无效实例
                            instanceBackupMap.put(instanceCacheKey, newInstanceUrlSet); // 替换当前备份为最新备份
                        }
//                        // 尝试恢复因为zk客户端意外断开导致的实例数据
//                        if ((!CollectionUtils.isEmpty(sourceInstances))) {
//                            tryToCompensate(taskDO, sourceNamingService, sourceInstances);
//                        }
                    } catch (Exception e) {
                        log.error("event process fail, taskId:{}", taskDO.getTaskId(), e);
                        metricsManager.recordError(MetricsStatisticsType.SYNC_ERROR);
                    }
                }
            });
            Set<String> services = new HashSet<>();
            services.add(taskDO.getServiceName());
            if (ALL_SERVICE_NAME_PATTERN.equals(taskDO.getServiceName())) {
                ListView<String> listView;
                try {
                    listView = sourceNamingService.getServicesOfServer(0, 1000);
                } catch (NullPointerException e) {  // 兼容 Nacos 内部的异常
                    listView = sourceNamingService.getServicesOfServer(0, 1000);
                }

                services = listView.getData().stream().filter(service -> {
                    try {
                        List<Instance> instanceList = sourceNamingService.getAllInstances(service);
                        return !instanceList.isEmpty() && needSync(instanceList.get(0).getMetadata());
                    } catch (NacosException e) {
                        return false;
                    }
                }).collect(Collectors.toSet());
            }
            for (String service : services) {
                sourceNamingService.subscribe(service, nacosListenerMap.get(taskDO.getTaskId()));
            }
            subscribeMap.put(taskDO.getTaskId(), services);
        } catch (Exception e) {
            log.error("sync task from nacos to zk was failed, taskId:{}", taskDO.getTaskId(), e);
            metricsManager.recordError(MetricsStatisticsType.SYNC_ERROR);
            return false;
        }
        return true;
    }

    String getRealServiceName(String serviceName) {
        return serviceName.split("@@")[1];
    }

    private List<Instance> getAllInstance(NamingService sourceNamingService, TaskDO taskDO) throws NacosException {
        List<Instance> list = new ArrayList<>();
        if (ALL_SERVICE_NAME_PATTERN.equals(taskDO.getServiceName())) {
            List<String> services = sourceNamingService.getServicesOfServer(0, 1000).getData();
            if (Objects.isNull(services)) {
                return list;
            }
            for (String service : services) {
                list.addAll(sourceNamingService.getAllInstances(service));
            }
        } else {
            list = sourceNamingService.getAllInstances(taskDO.getServiceName());
        }
        return list.stream().filter(instance -> needSync(instance.getMetadata())).collect(Collectors.toList());
    }

    protected void writeInstanceData(CuratorFramework zkClient, String zkInstancePath, Instance instance) throws Exception {
        zkClient.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL)
                .forPath(zkInstancePath);
    }

    private void tryToCompensate(TaskDO taskDO, NamingService sourceNamingService) {
        final PathChildrenCache pathCache = getPathCache(taskDO);
        if (pathCache.getListenable().size() != 0) { // 防止重复注册
            return;
        }
        pathCache.getListenable().addListener((zkClient, zkEvent) -> {
            if (zkEvent.getType() == PathChildrenCacheEvent.Type.CHILD_REMOVED) {
                return;
            }
            List<Instance> allInstances = new ArrayList<>(); // sourceNamingService.getAllInstances()
            for (Instance instance : allInstances) {
                String instanceUrl = buildSyncInstance(instance, taskDO);
                String zkInstancePath = zkEvent.getData().getPath();
                if (zkInstancePath.equals(instanceUrl)) {
                    writeInstanceData(zkClient, zkInstancePath, instance);
                    break;
                }
            }
        });
    }

    private void deleteInvalidInstances(String key, CuratorFramework client, Set<String> newInstanceUrlSet) {
        Set<String> instanceBackup = instanceBackupMap.getOrDefault(key, Sets.newHashSet());
        for (String instanceUrl : instanceBackup) {
            if (newInstanceUrlSet.contains(instanceUrl)) {
                continue;
            }
            deleteNode(client, instanceUrl);
        }
    }

    HashSet<String> getWaitingToAddInstance(TaskDO taskDO, CuratorFramework client, String service, List<Instance> sourceInstances) throws Exception {
        HashSet<String> waitingToAddInstance = new HashSet<>();
        for (Instance instance : sourceInstances) {
            if (needSync(instance.getMetadata())) {
                String instanceUrl = buildSyncInstance(instance, taskDO);
                if (null == client.checkExists().forPath(instanceUrl)) {
                    writeInstanceData(client, instanceUrl, instance);
                } else {
                    byte[] bytes = client.getData().forPath(instanceUrl);
                    if (bytes == null || bytes.length == 0) {
                        deleteNode(client, instanceUrl);
                        writeInstanceData(client, instanceUrl, instance);
                    }
                }
                waitingToAddInstance.add(instanceUrl);
            }
        }
        return waitingToAddInstance;
    }

    void deleteNode(CuratorFramework client, String path) {
        try {
            if (client.checkExists().forPath(path) != null) {
                client.delete().deletingChildrenIfNeeded().forPath(path);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    protected String buildSyncInstance(Instance instance, TaskDO taskDO) throws UnsupportedEncodingException {
        Map<String, String> metaData = new HashMap<>();
        metaData.putAll(instance.getMetadata());
        metaData.put(SkyWalkerConstants.DEST_CLUSTERID_KEY, taskDO.getDestClusterId());
        metaData.put(SkyWalkerConstants.SYNC_SOURCE_KEY,
            skyWalkerCacheServices.getClusterType(taskDO.getSourceClusterId()).getCode());
        metaData.put(SkyWalkerConstants.SOURCE_CLUSTERID_KEY, taskDO.getSourceClusterId());

        String servicePath = monitorPath.computeIfAbsent(taskDO.getTaskId(),
            key -> convertDubboProvidersPath(metaData.get(DubboConstants.INTERFACE_KEY)));

        return convertDubboFullPathForZk(metaData, servicePath, instance.getIp(), instance.getPort());
    }


    /**
     * 获取zk path child 监听缓存类
     *
     * @param taskDO 任务对象
     * @return zk节点操作缓存对象
     */
    private PathChildrenCache getPathCache(TaskDO taskDO) {
        return pathChildrenCacheMap.computeIfAbsent(taskDO.getTaskId(), (key) -> {
            try {
                PathChildrenCache pathChildrenCache = new PathChildrenCache(
                    zookeeperServerHolder.get(taskDO.getDestClusterId(), ""), monitorPath.get(key), false);
                pathChildrenCache.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);
                return pathChildrenCache;
            } catch (Exception e) {
                log.error("zookeeper path children cache start failed, taskId:{}", taskDO.getTaskId(), e);
                return null;
            }
        });

    }


}
