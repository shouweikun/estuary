package com.neighborhood.aka.laplace.estuary.core.util.zookeeper;

import org.I0Itec.zkclient.IZkConnection;
import org.I0Itec.zkclient.exception.ZkException;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


/**
 * Created by john_liu on 2019/3/11.
 */
public class EstuaryZookeeper implements IZkConnection {

    public static final Logger logger = LoggerFactory.getLogger(EstuaryZookeeper.class);
    private static final String SERVER_COMMA_1 = ";";
    private static final String SERVER_COMMA_2 = ",";
    private static final int DEFAULT_SESSION_TIMEOUT = 90000;

    private volatile ZooKeeper _zk = null;
    private Lock _zookeeperLock = new ReentrantLock();

    private final String _serverString;
    private final List<String> _servers;
    private final int _sessionTimeOut;

    public EstuaryZookeeper(String zkServers) {
        this(zkServers, DEFAULT_SESSION_TIMEOUT);
    }

    public EstuaryZookeeper(String zkServers, int sessionTimeOut) {
        String[] serverArray;
        if (zkServers.contains(SERVER_COMMA_1)) {
            serverArray = zkServers.split(SERVER_COMMA_1);
        } else serverArray = zkServers.split(SERVER_COMMA_2);
        _serverString = zkServers;
        _servers = Arrays.asList(serverArray);
        _sessionTimeOut = sessionTimeOut;
    }

    @Override
    public void connect(Watcher watcher) {
        _zookeeperLock.lock();
        try {
            if (_zk != null) {
                logger.warn("zk client has already been started");
            }
            try {
                logger.info("creating new zookeeper instance to connect to servers " + _servers + ".");
                _zk = new ZooKeeper(_servers.get(0), _sessionTimeOut, watcher);
            } catch (IOException e) {
                throw new ZkException("Unable to connect to " + _servers, e);
            }
        } finally {
            _zookeeperLock.unlock();
        }
    }

    @Override
    public void close() throws InterruptedException {
        _zookeeperLock.lock();
        try {
            if (_zk != null) {
                logger.info("closing zk connection to " + _servers);
                _zk.close();
                _zk = null;
            }
        } finally {
            _zookeeperLock.unlock();
        }
    }

    @Override
    public String create(String path, byte[] data, CreateMode mode) throws KeeperException, InterruptedException {
        return _zk.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, mode);
    }

    @Override
    public void delete(String path) throws InterruptedException, KeeperException {
        _zk.delete(path, -1);
    }

    @Override
    public boolean exists(String path, boolean watch) throws KeeperException, InterruptedException {
        return _zk.exists(path, watch) != null;
    }

    @Override
    public List<String> getChildren(String path, boolean watch) throws KeeperException, InterruptedException {
        return _zk.getChildren(path, watch);
    }

    @Override
    public byte[] readData(String path, Stat stat, boolean watch) throws KeeperException, InterruptedException {
        return _zk.getData(path, watch, stat);
    }

    public void writeData(String path, byte[] data) throws KeeperException, InterruptedException {
        writeData(path, data, -1);

    }

    @Override
    public void writeData(String path, byte[] data, int expectedVersion) throws KeeperException, InterruptedException {
        _zk.setData(path, data, expectedVersion);
    }

    @Override
    public ZooKeeper.States getZookeeperState() {
        return _zk != null ? _zk.getState() : null;
    }

    @Override
    public long getCreateTime(String path) throws KeeperException, InterruptedException {
        Stat stat = _zk.exists(path, false);
        if (stat != null) {
            return stat.getCtime();
        }
        return -1;
    }

    @Override
    public String getServers() {
        return _serverString;
    }
}
