package com.allen.hadoop.join;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by Administrator on 2017/6/5.
 */
public class Demo1 {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();

        Job job = Job.getInstance(configuration);
        job.setJarByClass(Demo1.class);

        job.setMapperClass(Demo1Mapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(InfoBean.class);

        job.setReducerClass(Demo1Reducer.class);
        job.setOutputKeyClass(InfoBean.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job,new Path("F:\\wc\\input"));

        FileOutputFormat.setOutputPath(job,new Path("F:\\wc\\output"));

        job.waitForCompletion(true);
    }
}

class Demo1Mapper extends Mapper<LongWritable,Text,Text,InfoBean>{

    InfoBean infoBean = new InfoBean();

    Text k = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //每次读取一个文件的时候，应该先要能拿到这个文件的文件名
        // 从而区分开学生信息以及考试信息
        FileSplit inputSplit = (FileSplit) context.getInputSplit();
        String fileName = inputSplit.getPath().getName();

        String[] split = value.toString().split("\t");

        //学生的基本信息
        if(fileName.startsWith("stu")){
            infoBean.setInfoBean(split[0],split[1],split[2],"","","0");
        }else{
            infoBean.setInfoBean(split[0],"","",split[1],split[2],"1");
        }

        k.set(split[0]);
        context.write(k,infoBean);
    }
}

class Demo1Reducer extends Reducer<Text,InfoBean,InfoBean,NullWritable>{

    @Override
    protected void reduce(Text key, Iterable<InfoBean> values, Context context) throws IOException, InterruptedException {
        InfoBean stuInfo = new InfoBean();

        ArrayList<InfoBean> scoreBeans = new ArrayList<InfoBean>();

        //需要先将学生信息与考试信息区分开来
        for(InfoBean infoBean:values){
            if(infoBean.getFlag().equals("0")){
                stuInfo.setInfoBean(infoBean.getId(),infoBean.getName(),infoBean.getSex(),"","","");
            }else{
                scoreBeans.add(new InfoBean(infoBean.getId(),"","",infoBean.getCname(),infoBean.getScore(),""));
            }
        }

        //将学生信息和考试信息join起来
        for(InfoBean infoBean:scoreBeans){
            infoBean.setName(stuInfo.getName());
            infoBean.setSex(stuInfo.getSex());
            context.write(infoBean,NullWritable.get());
        }
    }
}


