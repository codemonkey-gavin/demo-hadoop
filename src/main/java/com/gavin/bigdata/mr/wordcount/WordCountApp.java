package com.gavin.bigdata.mr.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.net.URI;

public class WordCountApp {
    public static void main(String[] args) {
        try {
            System.setProperty("HADOOP_USER_NAME", "root");
            Configuration configuration = new Configuration();
            configuration.set("fs.defaultFS", "hdfs://192.168.1.129:8020");

            // 创建一个Job
            Job job = Job.getInstance(configuration);
            // 设置主类
            job.setJarByClass(WordCountApp.class);
            // 设置自定义处理类Mapper
            job.setMapperClass(WordCountMapper.class);
            // 设置自定义处理类Reduce
            job.setReducerClass(WordCountReducer.class);
            // 设置Combiner
            // 优点：能减少IO，提升作业的执行性能
            // 局限性：比如求平均数会影响结果
            job.setCombinerClass(WordCountReducer.class);
            // 设置Mapper输出的类型
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);
            // 设置Reduce输出的类型
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);

            // 设置Mapper输出key和value的类型，作业输入和输出的路径
            FileInputFormat.setInputPaths(job, new Path("/wordcount/input"));
            FileOutputFormat.setOutputPath(job, new Path("/wordcount/output"));

            FileSystem fileSystem = FileSystem.get(new URI("hdfs://192.168.1.129:8020"), configuration, "root");
            Path outputpath = new Path("/wordcount/output");
            if (fileSystem.exists(outputpath)) {
                fileSystem.delete(outputpath, true);
            }
            //提交job
            boolean result = job.waitForCompletion(true);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
