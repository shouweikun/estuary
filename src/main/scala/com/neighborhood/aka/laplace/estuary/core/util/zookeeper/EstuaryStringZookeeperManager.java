package com.neighborhood.aka.laplace.estuary.core.util.zookeeper;

import com.alibaba.fastjson.JSON;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by john_liu on 2019/3/12.
 *
 * @author john_liu
 */
public class EstuaryStringZookeeperManager {
    private static Logger logger = LoggerFactory.getLogger(EstuaryStringZookeeperManager.class);
    final private EstuaryZkClient zkClient;
    final private AtomicBoolean connectStatus = new AtomicBoolean(false);

    public EstuaryStringZookeeperManager(EstuaryZkClient zkClient) {
        this.zkClient = zkClient;
    }

    public void start() {
        logger.info("EstuaryStringZookeeperManager start");
        Assert.notNull(zkClient);
        connectStatus.set(true);
    }

    public void stop() {
        logger.info("EstuaryStringZookeeperManager stop");
        try {
            zkClient.close();
        } catch (Exception e) {

        } finally {
            connectStatus.set(false);
        }
    }

    public boolean isConnected() {
        return connectStatus.get();
    }

    public String getStringBy(String destination) {
        String path = EstuaryZookeeperPathUtil.getParsePath(destination);
        byte[] data = zkClient.readData(path, true);
        if (data == null || data.length == 0) {
            return null;
        }
        return (String) JSON.parseObject(data, String.class);
    }

    public void persistStringBy(String destination, String value) {
        String path = EstuaryZookeeperPathUtil.getParsePath(destination);
        byte[] data = JSON.toJSONBytes(value);
        try {
            zkClient.writeData(path, data);
        } catch (ZkNoNodeException e) {
            zkClient.createPersistent(path, data, true);
        }
    }
}

