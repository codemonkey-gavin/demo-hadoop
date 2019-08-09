package com.gavin.bigdata.project.mrv1;

import com.gavin.bigdata.project.utils.IPParser;
import com.gavin.bigdata.project.utils.LogParser;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Map;

public class ProvinceStatisticsApp {
    public static void main(String[] args) {
        // Driver
        try {
            Configuration configuration = new Configuration();
            Path output = new Path("output/province");
            FileSystem fileSystem = FileSystem.get(configuration);
            if (fileSystem.exists(output)) {
                fileSystem.delete(output, true);
            }
            Job job = Job.getInstance(configuration);
            job.setJarByClass(ProvinceStatisticsApp.class);

            job.setMapperClass(MyMapper.class);
            job.setReducerClass(MyReducer.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(LongWritable.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(LongWritable.class);

            FileInputFormat.setInputPaths(job, new Path("input/vmware-1.log"));
            FileOutputFormat.setOutputPath(job, output);

            job.waitForCompletion(true);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    static class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        private LongWritable ONE = new LongWritable(1);
        private LogParser logParser;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            logParser = new LogParser();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String log = value.toString();
            Map<String, String> map = logParser.parse(log);
            String ip = map.get("ip");
            if (StringUtils.isNotBlank(ip)) {
                IPParser.RegionInfo regionInfo = IPParser.getInstance().analyseIp(ip);
                if (regionInfo != null) {
                    String province = regionInfo.getProvince();
                    if (StringUtils.isNotBlank(province)) {
                        context.write(new Text(province), ONE);
                    } else {
                        context.write(new Text("-"), ONE);
                    }
                } else {
                    context.write(new Text("-"), ONE);
                }
            } else {
                context.write(new Text("-"), ONE);
            }
        }
    }

    static class MyReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long count = 0;
            for (LongWritable value : values) {
                count++;
            }
            context.write(key, new LongWritable(count));
        }
    }
}
