package com.allen.hadoop.dfs;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Before;
import org.junit.Test;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Properties;
import java.util.TreeMap;

/**
 * Created by Administrator on 2017/5/31.
 */
public class CRUD {

    FileSystem fs = null;
    @Before
    public void init() throws Exception {
//        Properties properties = System.getProperties();
//        properties.setProperty("HADOOP_USER_NAME", "root");

      //  fs = FileSystem.get(new URI("hdfs://172.20.0.2:9000"), new Configuration(), "root");

             Properties properties = System.getProperties();
           properties.setProperty("HADOOP_USER_NAME", "root");

        Configuration  conf = new Configuration();
        // 指定HDFS路径的方法一
        conf.set("fs.defaultFS","hdfs://172.20.0.2:9000");
        conf.set("fs.hdfs.impl","org.apache.hadoop.hdfs.DistributedFileSystem");

        TreeMap map;

        try {
            fs = FileSystem.get(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

   // @Test
    public void upload() throws IOException {
        fs.copyFromLocalFile(new Path("D:\\pay.sql"),new Path("/pay.sql"));
        fs.close();
    }

    //@Test
    public void query() throws IOException {
        fs.copyToLocalFile(new Path("/pay.sql"),new Path("D:\\2.txt"));
        fs.close();
    }

    @Test
    public void stream() throws Exception{
        FSDataInputStream inputStream = fs.open(new Path("/pay.sql"));
        //IOUtils.copy(inputStream,System.out);
        byte[] buffer = new byte[1];
        OutputStream out=new FileOutputStream("d:\\24.txt");
        IOUtils.copyLarge(inputStream, out, 2, 3000,buffer);
        fs.close();
    }

    @Test
    public void fileStatus() throws Exception{
        //列出根目录下的所有的文件，包括目录
        //如果是文件的话，那么就是文件本身
        FileStatus[] fileStatuses = fs.listStatus(new Path("/pay.sql"));
        for(FileStatus statuses:fileStatuses){
            BlockLocation[] blockLocations = fs.getFileBlockLocations(statuses, 0L, statuses.getLen());

            for (BlockLocation bl:blockLocations){
                /*因为每一个bl有多个副本，所以返回的是一个数组类型的数据*/
                System.out.println(bl.getHosts());
                for (String name : bl.getNames()){
                    System.out.println(name);
                }

                System.out.println(bl.getTopologyPaths()[0]);

                //获取的是这个bl的长度和在文件中的偏移量

                System.out.println("bl.getLength"+bl.getLength());
                System.out.println("bl.getOffset"+bl.getOffset());
            }
        }
        fs.close();
    }

}
