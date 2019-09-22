package com.allen.hadoop.sort;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by Administrator on 2017/6/7.
 */
public class GlobalSort {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();

        configuration.set("key.indexRange","26");

        Job job = Job.getInstance(configuration);
        job.setNumReduceTasks(2);
        job.setJarByClass(GlobalSort.class);
        job.setMapperClass(GlobalSortMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setReducerClass(GlobalSortReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        job.setPartitionerClass(GlobalSortPartitioner.class);

        FileInputFormat.setInputPaths(job,new Path("F:\\wc\\input"));
        FileOutputFormat.setOutputPath(job,new Path("F:\\wc\\output"));
        job.waitForCompletion(true);
    }
}

class GlobalSortMapper extends Mapper<LongWritable,Text,Text,LongWritable>{
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //value是获取的一行的数据的内容，此处可以split
        String[] splits = value.toString().split(" ");
        for(String str : splits){
            context.write(new Text(str), new LongWritable(1L));
        }
    }
}


class GlobalSortPartitioner  extends Partitioner<Text,LongWritable> implements Configurable {

    private Configuration configuration = null;
    private int indexRange = 0;

    public int getPartition(Text text, LongWritable longWritable, int numPartitions) {
        //假如取值范围等于26的话，那么就意味着只需要根据第一个字母来划分索引
        int index = 0;
        if(indexRange==26){
            index = text.toString().toCharArray()[0]-'a';
        }else if(indexRange == 26*26 ){
            //这里就是需要根据前两个字母进行划分索引了
            char[] chars = text.toString().toCharArray();
            if (chars.length==1){
                index = (chars[0]-'a')*26;
            }
            index = (chars[0]-'a')*26+(chars[1]-'a');
        }
        int perReducerCount = indexRange/numPartitions;
        if(indexRange<numPartitions){
            return numPartitions;
        }

        for(int i = 0;i<numPartitions;i++){
            int min = i*perReducerCount;
            int max = (i+1)*perReducerCount-1;
            if(index>=min && index<=max){
                return i;
            }
        }
        //这里我们采用的是第一种不太科学的方法
        return numPartitions-1;

    }

    public void setConf(Configuration conf) {
        this.configuration = conf;
        indexRange = configuration.getInt("key.indexRange",26*26);
    }

    public Configuration getConf() {
        return configuration;
    }
}

class GlobalSortReducer extends Reducer<Text,LongWritable,Text,LongWritable>{
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        long count = 0;
        for(LongWritable value : values){
            count += value.get();
        }
        context.write(key, new LongWritable(count));
    }
}