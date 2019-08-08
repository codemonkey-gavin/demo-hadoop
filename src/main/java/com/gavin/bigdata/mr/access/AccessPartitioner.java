package com.gavin.bigdata.mr.access;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class AccessPartitioner extends Partitioner<Text, Access> {
    /**
     * @param text          手机号
     * @param access        统计结果
     * @param numPartitions 分区
     * @return
     */
    @Override
    public int getPartition(Text text, Access access, int numPartitions) {
        if (text.toString().startsWith("13")) {
            return 0;
        } else if (text.toString().startsWith("15")) {
            return 1;
        } else {
            return 2;
        }
    }
}
