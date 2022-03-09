package com.alibaba.nacossync.extension.impl;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.nacos.api.naming.NamingService;
import com.alibaba.nacos.api.naming.pojo.Instance;
import com.alibaba.nacossync.cache.SkyWalkerCacheServices;
import com.alibaba.nacossync.constant.ClusterTypeEnum;
import com.alibaba.nacossync.constant.FrameworkEnum;
import com.alibaba.nacossync.constant.SkyWalkerConstants;
import com.alibaba.nacossync.extension.annotation.NacosSyncService;
import com.alibaba.nacossync.extension.holder.NacosServerHolder;
import com.alibaba.nacossync.extension.holder.ZookeeperServerHolder;
import com.alibaba.nacossync.pojo.model.TaskDO;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.utils.CloseableUtils;
import org.springframework.beans.factory.annotation.Autowired;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static com.alibaba.nacossync.util.DubboConstants.ALL_SERVICE_NAME_PATTERN;
import static com.alibaba.nacossync.util.DubboConstants.ZOOKEEPER_SEPARATOR;
import static java.lang.String.format;

@Slf4j
@NacosSyncService(sourceCluster = ClusterTypeEnum.ZK, destinationCluster = ClusterTypeEnum.NACOS, framework = FrameworkEnum.SPRING_CLOUD)
public class SpringCloudZookeeperSyncToNacosServiceImpl extends ZookeeperSyncToNacosServiceImpl{

    private static final String SPRING_CLOUD_ROOT_PATH = "/services";

    private static final String SPRING_CLOUD_SERVICE_PATH_FORMAT = StringUtils.join(new String[] {SPRING_CLOUD_ROOT_PATH, "%s"}, ZOOKEEPER_SEPARATOR);

    private static final String SPRING_CLOUD_SERVICE_INSTANCE_PATH_FORMAT = StringUtils.join(new String[] {SPRING_CLOUD_ROOT_PATH, "%s", "%s"}, ZOOKEEPER_SEPARATOR);

    private volatile Map<String, TreeCache> treeCacheMap = new ConcurrentHashMap<>();

    private final ZookeeperServerHolder zookeeperServerHolder;

    private final NacosServerHolder nacosServerHolder;

    private final SkyWalkerCacheServices skyWalkerCacheServices;

    private Map<String, AtomicBoolean> enabledMap = new ConcurrentHashMap<>();

    @Autowired
    public SpringCloudZookeeperSyncToNacosServiceImpl(ZookeeperServerHolder zookeeperServerHolder,
                                                      NacosServerHolder nacosServerHolder,
                                                      SkyWalkerCacheServices skyWalkerCacheServices
    ) {
        super(zookeeperServerHolder, nacosServerHolder, skyWalkerCacheServices);
        this.zookeeperServerHolder = zookeeperServerHolder;
        this.nacosServerHolder = nacosServerHolder;
        this.skyWalkerCacheServices = skyWalkerCacheServices;
    }

    @Override
    public boolean sync(TaskDO taskDO) {
        enabledMap.putIfAbsent(taskDO.getTaskId(), new AtomicBoolean(true));
        if (!treeCacheMap.containsKey(taskDO.getTaskId())) {
            registerZkNodeListener(taskDO);
            return true;
        }
        registerAllInstance(taskDO.getServiceName(), taskDO);
        return true;
    }

    @Override
    public boolean delete(TaskDO taskDO) {
        CloseableUtils.closeQuietly(treeCacheMap.get(taskDO.getTaskId()));
        deregisterAllInstance(taskDO.getServiceName(), taskDO);
        enabledMap.getOrDefault(taskDO.getTaskId(), new AtomicBoolean(false)).set(false);
        return true;
    }

    private synchronized void registerZkNodeListener(TaskDO taskDO) {
        treeCacheMap.computeIfAbsent(taskDO.getTaskId(), s -> {
            TreeCache treeCache = new TreeCache(zookeeperServerHolder.get(taskDO.getSourceClusterId(), ""),
                    SPRING_CLOUD_ROOT_PATH);
            treeCache.getListenable().addListener((client, event) -> processEvent(client, event, taskDO));
            try {
                treeCache.start();
                return treeCache;
            } catch (Exception e) {
                throw new RuntimeException("start listener failed!", e);
            }
        });
    }

    private void processEvent(CuratorFramework client, TreeCacheEvent event, TaskDO taskDO) {
        if (!enabledMap.get(taskDO.getTaskId()).get()) {
            return;
        }
        TreeCacheEvent.Type type = event.getType();
        if (event.getData() == null || event.getData().getData() == null || event.getData().getData().length == 0) {
            return;
        }
        byte[] bytes = event.getData().getData();
        Instance newInstance = buildInstance(JSON.parseObject(new String(bytes, StandardCharsets.UTF_8)), taskDO);
        if (newInstance == null) {
            return;
        }
        switch (type) {
            case NODE_ADDED:
            case NODE_UPDATED:
                registerInstanceIfAbsent(newInstance, taskDO);
                break;
            case NODE_REMOVED:
                deregisterInstance(newInstance, taskDO);
                break;
            default:
                break;
        }
    }

    @SneakyThrows
    private void registerInstanceIfAbsent(Instance toRegisterInstance, TaskDO taskDO) {
        NamingService destNamingService = nacosServerHolder.get(taskDO.getDestClusterId(), taskDO.getNameSpace());
        List<Instance> instanceList = destNamingService.getAllInstances(toRegisterInstance.getServiceName());
        List<Instance> toRemoveInstanceList = instanceList.stream().filter(instance -> instance.getIp().equals(toRegisterInstance.getIp()) && instance.getPort() == toRegisterInstance.getPort()).collect(Collectors.toList());
        if (!toRemoveInstanceList.isEmpty()) {
            for (Instance instance : toRemoveInstanceList) {
                destNamingService.deregisterInstance(instance.getServiceName(), instance);
            }
        }
        destNamingService.registerInstance(toRegisterInstance.getServiceName(), toRegisterInstance);
    }

    @SneakyThrows
    private void deregisterInstance(Instance toRemovedInstance, TaskDO taskDO) {
        NamingService destNamingService = nacosServerHolder.get(taskDO.getDestClusterId(), taskDO.getNameSpace());
        List<Instance> instanceList = destNamingService.getAllInstances(toRemovedInstance.getServiceName());
        for (Instance instance : instanceList) {
            if (!toRemovedInstance.getIp().equals(instance.getIp()) || toRemovedInstance.getPort() != instance.getPort()) {
                continue;
            }
            destNamingService.deregisterInstance(toRemovedInstance.getServiceName(), instance);
        }
    }

    private void registerAllInstance(String updatedServiceName, TaskDO taskDO) {
        if (!ALL_SERVICE_NAME_PATTERN.equals(taskDO.getServiceName()) && !taskDO.getServiceName().equals(updatedServiceName)) {
            return;
        }
        CuratorFramework zk =  zookeeperServerHolder.get(taskDO.getSourceClusterId(), null);
        NamingService destNamingService = nacosServerHolder.get(taskDO.getDestClusterId(), taskDO.getNameSpace());
        if (!ALL_SERVICE_NAME_PATTERN.equals(updatedServiceName)) {
            registerAllInstanceByServiceName(zk, destNamingService, updatedServiceName, taskDO);
        } else {
            try {
                zk.getChildren().forPath(SPRING_CLOUD_ROOT_PATH).forEach(s -> registerAllInstanceByServiceName(zk, destNamingService, s, taskDO));
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }
    }

    private void registerAllInstanceByServiceName(CuratorFramework zk, NamingService destNamingService, String updatedServiceName, TaskDO taskDO){
        try {
            List<String> instancePathList = zk.getChildren().forPath(format(SPRING_CLOUD_SERVICE_PATH_FORMAT, updatedServiceName));
            for (String instancePath : instancePathList) {
                byte[] bytes = zk.getData().forPath(format(SPRING_CLOUD_SERVICE_INSTANCE_PATH_FORMAT, updatedServiceName, instancePath));
                String instanceJsonStr = new String(bytes, StandardCharsets.UTF_8);
                log.info("instanceJson, instancePath: {}, data: {}", instancePath, instanceJsonStr);
                if (StringUtils.isBlank(instanceJsonStr)) {
                    continue;
                }
                Instance instance = buildInstance(JSON.parseObject(instanceJsonStr), taskDO);
                if (instance != null) {
                    destNamingService.registerInstance(updatedServiceName, instance);
                }
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    private void deregisterAllInstance(String updatedServiceName, TaskDO taskDO) {
        if(!ALL_SERVICE_NAME_PATTERN.equals(taskDO.getServiceName()) && !taskDO.getServiceName().equals(updatedServiceName)) {
            return;
        }
        NamingService destNamingService = nacosServerHolder.get(taskDO.getDestClusterId(), taskDO.getNameSpace());
        if (!ALL_SERVICE_NAME_PATTERN.equals(updatedServiceName)) {
            deregisterAllInstanceByServiceName(destNamingService, updatedServiceName, taskDO);
        } else {
            try {
                CuratorFramework zk = zookeeperServerHolder.get(taskDO.getSourceClusterId(), "");
                zk.getChildren().forPath(SPRING_CLOUD_ROOT_PATH).forEach(s -> deregisterAllInstanceByServiceName(destNamingService, s, taskDO));
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }
    }

    private void deregisterAllInstanceByServiceName(NamingService destNamingService, String updatedServiceName, TaskDO taskDO) {
        try {
            List<Instance> instanceList = destNamingService.getAllInstances(updatedServiceName);
            for (Instance instance : instanceList) {
                if (!needSync(instance.getMetadata())) {
                    destNamingService.deregisterInstance(updatedServiceName, instance);
                }
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    private Instance buildInstance(JSONObject parseObject, TaskDO taskDO) {
        Instance temp = new Instance();
        temp.setInstanceId(parseObject.getString("id"));
        temp.setIp(parseObject.getString("address"));
        temp.setPort(parseObject.getInteger("port"));
        temp.setServiceName(parseObject.getString("name"));
        temp.setWeight( 1D);
        temp.setHealthy(true);

        Map<String, String> metaData = new HashMap<>();
        JSONObject payload =  parseObject.getJSONObject("payload");
        if (Objects.nonNull(payload)) {
            JSONObject metaMap = payload.getJSONObject("metadata");
            if (Objects.nonNull(metaMap)) {
                metaMap.forEach((k, v) -> metaData.put(k, Objects.isNull(v) ? null : String.valueOf(v)));
            }
            if (!needSync(metaData)) {
                return null;
            }
        }
        metaData.put(SkyWalkerConstants.DEST_CLUSTERID_KEY, taskDO.getDestClusterId());
        metaData.put(SkyWalkerConstants.SYNC_SOURCE_KEY,
                skyWalkerCacheServices.getClusterType(taskDO.getSourceClusterId()).getCode());
        metaData.put(SkyWalkerConstants.SOURCE_CLUSTERID_KEY, taskDO.getSourceClusterId());
        temp.setMetadata(metaData);
        return temp;
    }
}
