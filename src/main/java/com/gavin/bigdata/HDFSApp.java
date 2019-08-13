package com.gavin.bigdata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.net.URI;

public class HDFSApp {
    private static final String HDFS_URI = "hdfs://192.168.1.121:8020";
    Configuration configuration = null;
    FileSystem fileSystem = null;

    @Before
    public void setUp() throws Exception {
        System.out.println("-------------setUp-------------");
        configuration = new Configuration();
        // 副本数量
        configuration.set("dfs.replication", "1");

        /**
         * 构造一个访问指定HDFS系统的客户端对象
         * 第一个参数：HDFS的URI
         * 第二个参数：客户端指定的配置参数
         * 第三个参数：客户端身份验证
         */
        fileSystem = FileSystem.get(new URI(HDFS_URI), configuration, "root");
    }

    /**
     * 创建HDFS文件夹
     */
    @Test
    public void mkdir() throws Exception {
        boolean bl = fileSystem.mkdirs(new Path("/hdfsapi"));
        System.out.println("执行结果" + bl);
    }

    /**
     * 查看HDFS内容
     */
    @Test
    public void text() throws Exception {
        FSDataInputStream inputStream = fileSystem.open(new Path("/README.txt"));
        IOUtils.copyBytes(inputStream, System.out, 1024);
    }

    /**
     * 创建文件
     */
    @Test
    public void create() throws Exception {
        FSDataOutputStream inputStream = fileSystem.create(new Path("/hello.txt"));
        inputStream.writeUTF("hello world!");
        inputStream.flush();
        inputStream.close();
    }

    /**
     * 重命名
     */
    @Test
    public void rename() throws Exception {
        boolean bl = fileSystem.rename(new Path("/h.txt"), new Path("/b.txt"));
        System.out.println(bl);
    }

    /**
     * 删除文件
     */
    @Test
    public void delete() throws Exception {
        boolean bl = fileSystem.delete(new Path("/hdfsapi/jdk.tgz"), true);
        System.out.println(bl);
    }

    /**
     * 上传文件
     */
    @Test
    public void copyFromLocalFile() throws Exception {
        fileSystem.copyFromLocalFile(new Path("E:/hello.txt"), new Path("/input"));
    }

    /**
     * 上传大文件
     */
    @Test
    public void copyFromLocalBigFile() throws Exception {
        InputStream stream = new BufferedInputStream(new FileInputStream(new File("F:/Soft/Java/jdk-12.0.1_linux-x64_bin.tar.gz")));
        FSDataOutputStream outputStream = fileSystem.create(new Path("/hdfsapi/jdk.tgz"), new Progressable() {
            @Override
            public void progress() {
                System.out.println("*");
            }
        });
        IOUtils.copyBytes(stream, outputStream, 2000);
    }

    /**
     * 查看文件信息
     */
    @Test
    public void listStatus() throws Exception {
        FileStatus[] fileStatus = fileSystem.listStatus(new Path("/hdfsapi"));
        for (FileStatus file : fileStatus) {
            String isDir = file.isDirectory() ? "文件夹" : "文件";
            String permission = file.getPermission().toString();
            short replication = file.getReplication();
            long length = file.getLen();
            String path = file.getPath().toString();
            System.out.println(isDir + "\t" + permission + "\t" + replication + "\t" + length + "\t" + path);
        }
    }

    /**
     * 递归查看文件信息
     */
    @Test
    public void listFiles() throws Exception {
        RemoteIterator<LocatedFileStatus> listFiles = fileSystem.listFiles(new Path("/"), true);
        while (listFiles.hasNext()) {
            LocatedFileStatus file = listFiles.next();
            String isDir = file.isDirectory() ? "文件夹" : "文件";
            String permission = file.getPermission().toString();
            short replication = file.getReplication();
            long length = file.getLen();
            String path = file.getPath().toString();
            System.out.println(isDir + "\t" + permission + "\t" + replication + "\t" + length + "\t" + path);
        }
    }

    @After
    public void tearDown() {
        System.out.println("-------------tearDown-------------");
        configuration = null;
        fileSystem = null;
    }
}
