package com.gavin.bigdata.hdfs;

public interface IHDFSMapper {
    /**
     * @param line    读取的每一行字符串
     * @param context 上下文/缓存
     */
    void map(String line, HDFSContext context);
}
