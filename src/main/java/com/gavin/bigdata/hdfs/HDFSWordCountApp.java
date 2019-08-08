package com.gavin.bigdata.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * 使用HDFS API完成Word Count统计（词频统计）
 */
public class HDFSWordCountApp {
    public static void main(String[] args) {
        try {
            System.out.println("++++++++++++++++++执行开始+++++++++++++++++");
            Properties properties = ParamsUtils.getProperties();
            // 读取文件
            Path input = new Path(properties.getProperty(Constants.INPUT_PATH));
            FileSystem fs = FileSystem.get(new URI(properties.getProperty(Constants.HDFS_URI)), new Configuration(), "root");
            RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(input, false);

            HDFSContext context = new HDFSContext();
            //通过反射实例化对象
            Class<?> clazz = Class.forName(properties.getProperty("MAPPER_CLASS"));
            IHDFSMapper mapper = (IHDFSMapper)clazz.newInstance();
            while (iterator.hasNext()) {
                LocatedFileStatus file = iterator.next();
                FSDataInputStream inputStream = fs.open(file.getPath());
                BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
                String line = "";
                while (null != (line = reader.readLine())) {
                    // 词频统计
                    mapper.map(line, context);
                }

                reader.close();
                inputStream.close();
            }

            // 缓存处理结果
            Map<Object, Object> contextMap = context.getCacheMap();

            // 将结果写入到HDFS
            Path output = new Path(properties.getProperty(Constants.OUTPUT_PATH));
            FSDataOutputStream outputStream = fs.create(new Path(output, new Path(properties.getProperty(Constants.OUTPUT_FILE))));
            Set<Map.Entry<Object, Object>> entries = contextMap.entrySet();
            for (Map.Entry<Object, Object> entry : entries) {
                outputStream.write((entry.getKey() + "\t" + entry.getValue() + "\n").getBytes());
            }
            outputStream.close();
            fs.close();

        } catch (Exception e) {
            e.printStackTrace();
        }

        System.out.println("++++++++++++++++++执行结束+++++++++++++++++");
    }
}
