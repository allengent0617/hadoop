package com.allen.hadoop.join;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;
import java.net.URI;
import java.util.Hashtable;
import java.util.Map;

/**
 * Created by Administrator on 2017/6/5.
 */
public class Demo2 {
    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();

        Job job = Job.getInstance(configuration);
        job.setJarByClass(Demo2.class);

        job.setMapperClass(Demo2Mapper.class);
        job.setMapOutputKeyClass(InfoBean.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setNumReduceTasks(0);

        //将文件上传到每一个task运行节点的工作目录下
        job.addCacheFile(new URI("file:/F:/stu01.txt"));

        FileInputFormat.setInputPaths(job,new Path("F:\\wc\\input"));
        FileOutputFormat.setOutputPath(job,new Path("F:\\wc\\output"));
        job.waitForCompletion(true);
    }
}

class Demo2Mapper extends Mapper<LongWritable,Text,Text,NullWritable>{

    Map<String,String> stuInfo = new Hashtable<String, String>();
    Text t = new Text();
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        URI[] cacheFiles = context.getCacheFiles();

        //从当前工作目录下读取出学生信息，数据较少的情况下，可以直接放在内存中
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(new File(cacheFiles[0]))));

        String line;
        while((line=reader.readLine())!=null){
            String[] split = line.split("\t");
            stuInfo.put(split[0],split[1]+"\t"+split[2]);
        }
        reader.close();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("\t");
        if(stuInfo.containsKey(split[0])){
            t.set(stuInfo.get(split[0])+"\t"+value.toString());
            context.write(t,NullWritable.get());
        }
    }
}
