package com.alibaba.nacossync.extension.impl;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.nacos.api.naming.pojo.Instance;
import com.alibaba.nacossync.cache.SkyWalkerCacheServices;
import com.alibaba.nacossync.constant.ClusterTypeEnum;
import com.alibaba.nacossync.constant.FrameworkEnum;
import com.alibaba.nacossync.constant.SkyWalkerConstants;
import com.alibaba.nacossync.extension.annotation.NacosSyncService;
import com.alibaba.nacossync.extension.holder.NacosServerHolder;
import com.alibaba.nacossync.extension.holder.ZookeeperServerHolder;
import com.alibaba.nacossync.pojo.model.TaskDO;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;


@Slf4j
@NacosSyncService(sourceCluster = ClusterTypeEnum.NACOS, destinationCluster = ClusterTypeEnum.ZK, framework = FrameworkEnum.SPRING_CLOUD)
public class SpringCloudNacosSyncToZookeeperServiceImpl extends NacosSyncToZookeeperServiceImpl {

    private static final String SPRING_CLOUD_INSTANCE_PATH_FORMAT = "/services/%s/%s";

    private static final String SPRING_CLOUD_SERVICE_PATH_FORMAT = "/services/%s";

    public SpringCloudNacosSyncToZookeeperServiceImpl(SkyWalkerCacheServices skyWalkerCacheServices, NacosServerHolder nacosServerHolder, ZookeeperServerHolder zookeeperServerHolder) {
        super(skyWalkerCacheServices, nacosServerHolder, zookeeperServerHolder);
    }

    @Override
    public boolean sync(TaskDO taskDO) {
        return super.sync(taskDO);
    }

    @Override
    protected String buildSyncInstance(Instance instance, TaskDO taskDO) {
        Map<String, String> metaData = instance.getMetadata();
        metaData.put(SkyWalkerConstants.DEST_CLUSTERID_KEY, taskDO.getDestClusterId());
        metaData.put(SkyWalkerConstants.SYNC_SOURCE_KEY, skyWalkerCacheServices.getClusterType(taskDO.getSourceClusterId()).getCode());
        metaData.put(SkyWalkerConstants.SOURCE_CLUSTERID_KEY, taskDO.getSourceClusterId());
        String service = getRealServiceName(instance.getServiceName());
        String instanceId = instance.getIp() + "-" + instance.getPort();
        String path = String.format(SPRING_CLOUD_INSTANCE_PATH_FORMAT, service, instanceId);
        monitorPath.put(taskDO.getTaskId(), path);
        return path;
    }

    @Override
    HashSet<String> getWaitingToAddInstance(TaskDO taskDO, CuratorFramework client, String service, List<Instance> sourceInstances) throws Exception {
        if (!sourceInstances.isEmpty() && needSync(sourceInstances.get(0).getMetadata())) {         //如果 Zookeeper 中的实例数量与真实的不一致，则删除Zookeeper中该服务下所有实例
            if (client.checkExists().forPath(getServicePath(service)) != null) {
                List<String> instanceNames = client.getChildren().forPath(getServicePath(service));
                if (instanceNames.size() > 0 && instanceNames.size() != sourceInstances.size()) {
                    deleteNode(client, getServicePath(service));
                }
            }
        }
        if (!sourceInstances.isEmpty()) {
            return super.getWaitingToAddInstance(taskDO, client, service, sourceInstances);
        }
        String servicePath = getServicePath(service);
        if (null == client.checkExists().forPath(servicePath)) {
            return super.getWaitingToAddInstance(taskDO, client, service, sourceInstances);
        }
        List<String> instanceNames = client.getChildren().forPath(servicePath);
        if (instanceNames.isEmpty()) {
            return super.getWaitingToAddInstance(taskDO, client, service, sourceInstances);
        }
        //如果Nacos中没有实例， 且Zookeeper中存在实例，则删除Zookeeper中该服务下的所有实例
        for (String instanceName: instanceNames) {
            String instancePath = getInstancePath(service, instanceName);
            if (!needSync(deserializeMetaMap(client, instancePath))) {  //仅删除同步过来的数据
                deleteNode(client, getServicePath(service));
            }
        }
        return super.getWaitingToAddInstance(taskDO, client, service, sourceInstances);
    }

    private Map<String, String> deserializeMetaMap(CuratorFramework client, String instancePath) {
        try {
            byte[] bytes = client.getData().forPath(instancePath);
            if (bytes.length == 0) {
                return new HashMap<>();
            }
            JSONObject jsonObject = JSON.parseObject(new String(bytes, StandardCharsets.UTF_8));
            return jsonObject.getJSONObject("payload").getJSONObject("metadata").entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, entry -> String.valueOf(entry.getValue())));
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return new HashMap<>();
        }
    }

    private String getInstancePath(String service, String instanceName) {
        return String.format(SPRING_CLOUD_INSTANCE_PATH_FORMAT, service, instanceName);
    }

    private String getServicePath(String service) {
        return String.format(SPRING_CLOUD_SERVICE_PATH_FORMAT, service);
    }

    @Override
    protected void writeInstanceData(CuratorFramework zkClient, String zkInstancePath, Instance instance) throws Exception {
        Map<String, Object> data = new HashMap<>();
        String service = getRealServiceName(instance.getServiceName());
        String instanceId = instance.getIp() + "-" + instance.getPort();
        data.put("name", service);
        data.put("id", instanceId);
        data.put("address", instance.getIp());
        data.put("port", instance.getPort());
        data.put("sslPort", null);

        data.put("payload", new HashMap<String, Object>(){
            {
                put("@class", "org.springframework.cloud.zookeeper.discovery.ZookeeperInstance");
                put("id", service + "-1");
                put("name", service);
                put("metadata", instance.getMetadata());
            }
        });

        data.put("registrationTimeUTC", System.currentTimeMillis());
        data.put("serviceType", "DYNAMIC");
        data.put("uriSpec", new HashMap<String, Object>(){
            {
                put("parts", new ArrayList<Map>(){
                    {
                        add(new HashMap(){
                            {
                                put("value", "scheme");
                                put("variable", true);
                            }
                        });
                        add(new HashMap(){
                            {
                                put("value", "://");
                                put("variable", false);
                            }
                        });
                        add(new HashMap(){
                            {
                                put("value", "address");
                                put("variable", true);
                            }
                        });
                        add(new HashMap(){
                            {
                                put("value", ":");
                                put("variable", false);
                            }
                        });
                        add(new HashMap(){
                            {
                                put("value", "port");
                                put("variable", true);
                            }
                        });
                    }
                });
            }
        });

        zkClient.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL)
                .forPath(zkInstancePath, JSON.toJSONString(data).getBytes(StandardCharsets.UTF_8));
    }

}
