package com.gavin.bigdata.project.mrv2;

import com.gavin.bigdata.project.mrv1.PageStatisticsApp;
import com.gavin.bigdata.project.utils.Content;
import com.gavin.bigdata.project.utils.LogParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Map;

public class ETLApp {
    public static void main(String[] args) {
// Driver
        try {
            Configuration configuration = new Configuration();
            Path output = new Path("output/etl");
            FileSystem fileSystem = FileSystem.get(configuration);
            if (fileSystem.exists(output)) {
                fileSystem.delete(output, true);
            }
            Job job = Job.getInstance(configuration);
            job.setJarByClass(ETLApp.class);

            job.setMapperClass(MyMapper.class);

            job.setMapOutputKeyClass(NullWritable.class);
            job.setMapOutputValueClass(Text.class);

            FileInputFormat.setInputPaths(job, new Path("input/vmware-1.log"));
            FileOutputFormat.setOutputPath(job, output);

            job.waitForCompletion(true);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    static class MyMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
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
            String country = map.get("country");
            String province = map.get("province");
            String city = map.get("city");
            String url = map.get("url");
            String time = map.get("time");
            String pageId = Content.getPageId(url);

            StringBuffer sb = new StringBuffer();
            sb.append(ip).append("\t");
            sb.append(country).append("\t");
            sb.append(province).append("\t");
            sb.append(city).append("\t");
            sb.append(url).append("\t");
            sb.append(time).append("\t");
            sb.append(pageId).append("\t");
            context.write(NullWritable.get(), new Text(sb.toString()));
        }
    }
}
