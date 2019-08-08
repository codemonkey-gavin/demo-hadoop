package com.gavin.bigdata.mr.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountLocalApp {
    public static void main(String[] args) {
        try {
            Configuration configuration = new Configuration();
            // 创建一个Job
            Job job = Job.getInstance(configuration);
            // 设置主类
            job.setJarByClass(WordCountLocalApp.class);
            // 设置自定义处理类Mapper
            job.setMapperClass(WordCountMapper.class);
            // 设置自定义处理类Reduce
            job.setReducerClass(WordCountReducer.class);
            // 设置Mapper输出的类型
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);
            // 设置Reduce输出的类型
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);

            // 设置Mapper输出key和value的类型，作业输入和输出的路径
            FileInputFormat.setInputPaths(job, new Path("input/hello.txt"));
            FileOutputFormat.setOutputPath(job, new Path("output"));

            //提交job
            boolean result = job.waitForCompletion(true);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
