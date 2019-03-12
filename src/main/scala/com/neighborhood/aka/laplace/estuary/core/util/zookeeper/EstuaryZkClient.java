package com.neighborhood.aka.laplace.estuary.core.util.zookeeper;

import com.alibaba.otter.canal.common.zookeeper.ZkClientx;
import com.google.common.base.Function;
import com.google.common.collect.MigrateMap;
import org.I0Itec.zkclient.ZkClient;

import java.util.Map;

/**
 * Created by john_liu on 2019/3/11.
 *
 * @author neighborhood.aka.laplace
 */
public class EstuaryZkClient extends ZkClient {

    private static Map<String, EstuaryZkClient> clients = MigrateMap.makeComputingMap(new Function<String, EstuaryZkClient>() {
        @Override
        public EstuaryZkClient apply(String servers) {
            return new EstuaryZkClient(servers);
        }
    });

    public static EstuaryZkClient getZkClient(String servers) {
        return clients.get(servers);
    }

    public EstuaryZkClient(String serverstring){
        this
    }
}
