package com.neighborhood.aka.laplace.estuary.bean.key;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * Created by john_liu on 2018/5/10.
 */
public class MultipleJsonKeyPartitionerJava implements Partitioner {
    private int partitionByPrimaryKey(Object key, int partitions) {
        return key.hashCode() % partitions;
    }

    private int partitionByMod(long mod, int partitions) {
        return (int) (mod % partitions);
    }

    private int partitionByDbAndTable(String db, String tb, int partitions) {
        return (db + "-" + tb).hashCode() % partitions;
    }

    @Override

    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        //todo
        BinlogKey theKey = (BinlogKey) key;
        int partitions = cluster.partitionCountForTopic(topic);
        return partitionByPrimaryKey(theKey.getPrimaryKeyValue(), partitions);
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
