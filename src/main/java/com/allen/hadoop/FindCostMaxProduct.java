package com.allen.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by Administrator on 2017/6/5.
 */
public class FindCostMaxProduct {
    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();

        Job job = Job.getInstance(configuration);
        job.setJarByClass(FindCostMaxProduct.class);

        job.setMapperClass(FindCostMaxProductMapper.class);
        job.setMapOutputKeyClass(OrderProduct.class);
        job.setMapOutputValueClass(NullWritable.class);

        //job.setNumReduceTasks(7);
        job.setGroupingComparatorClass(OrderGroupComparator.class);

        
//
       job.setPartitionerClass(OrderPartitioner.class);

        job.setReducerClass(FindCostMaxProductReducer.class);
        job.setOutputKeyClass(OrderProduct.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job,new Path("F:\\hdp\\order\\input"));
        FileOutputFormat.setOutputPath(job,new Path("F:\\hdp\\order\\output"));

        job.waitForCompletion(true);
    }
}


class FindCostMaxProductMapper extends Mapper<LongWritable,Text,OrderProduct,NullWritable>{


    List<OrderProduct> list = new ArrayList<OrderProduct>();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("\t");
        OrderProduct orderProduct = new OrderProduct();
        orderProduct.setOrder_id(split[0]);
        orderProduct.setPdt_id(split[1]);
        orderProduct.setMoney(Double.valueOf(split[2]));
        context.write(orderProduct,NullWritable.get());
        list.add(orderProduct);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        Collections.sort(list);
        System.out.println("1");
    }
}

class FindCostMaxProductReducer extends Reducer<OrderProduct,NullWritable,OrderProduct,NullWritable>{
    @Override
    protected void reduce(OrderProduct key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        context.write(key,NullWritable.get());
    }
}

class OrderPartitioner extends Partitioner<OrderProduct,NullWritable>{

    public int getPartition(OrderProduct orderProduct, NullWritable nullWritable, int numPartitions) {
        return orderProduct.getOrder_id().hashCode() % numPartitions;
    }
}


/*
默认情况下，即使将order_id相同的订单分配到了同一个reduce中，但是作为key的他们却不会是在同一个组中
不想<a.1><a,1><a,1>这样三个是在同一个组中的
 */
class OrderGroupComparator extends WritableComparator{

    public OrderGroupComparator(){
        super(OrderProduct.class,true);
    }

    //注意重写的需要是参数为WritableComparable类型的方法，因为其还有一个重载的参数类型为Object的方法
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        OrderProduct o1 = (OrderProduct) a;
        OrderProduct o2 = (OrderProduct) b;
        return o1.getOrder_id().compareTo(o2.getOrder_id());
    }

}