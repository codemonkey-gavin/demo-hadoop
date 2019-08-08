package com.gavin.bigdata.hdfs;

import java.util.HashMap;
import java.util.Map;

public class HDFSContext {
    private Map<Object, Object> cacheMap = new HashMap<>();

    public Map<Object, Object> getCacheMap() {
        return cacheMap;
    }

    /**
     * 写
     *
     * @param key   单词
     * @param value 次数
     */
    public void write(Object key, Object value) {
        cacheMap.put(key, value);
    }

    public Object get(Object key) {
        return cacheMap.get(key);
    }
}
