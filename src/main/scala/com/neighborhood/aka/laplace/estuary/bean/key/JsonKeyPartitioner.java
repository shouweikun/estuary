package com.neighborhood.aka.laplace.estuary.bean.key;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * Created by duming on 16/12/8.
 */
public class JsonKeyPartitioner implements Partitioner {
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        BaseDataJsonKey opsKey = (BaseDataJsonKey) key;
        int tmp = (int) opsKey.getSyncTaskSequence() % cluster.partitionCountForTopic(topic);

        //根据唯一键值来进行数据分发, 这个partioion 值必 须在kafka的partition范围内, 如果不在, 会报TimeoutException, 并且得不到任何提示.
        return tmp < 0 ? -tmp : tmp;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
