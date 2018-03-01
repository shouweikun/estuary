package com.neighborhood.aka.laplce.estuary.bean.key;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;


import java.io.IOException;
import java.util.Map;

/**
 * Created by duming on 16/12/7.
 */
public class JsonKeySerializer implements Serializer
{
    private ObjectMapper mapper = new ObjectMapper();
    @Override
    public void configure(Map map, boolean b) {

    }

    @Override
    public byte[] serialize(String s, Object o) {
        try {
            return mapper.writeValueAsBytes(o);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public void close() {

    }
}
