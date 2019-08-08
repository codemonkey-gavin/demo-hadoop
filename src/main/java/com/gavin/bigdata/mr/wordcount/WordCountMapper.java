package com.gavin.bigdata.mr.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 词频统计功能 Mapper
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        // 把value对应的行数据按照指定的分隔符拆开
        String[] words = value.toString().split(" ");
        for (String word : words) {
            // (hello,1)  (world,1)
            context.write(new Text(word.toLowerCase()), new IntWritable(1));
        }
    }
}
