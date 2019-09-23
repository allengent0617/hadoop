/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.allen.hadoop.dfs;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Properties;
import java.util.StringTokenizer;

public class WordCountHdfs {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
//    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
//    if (otherArgs.length < 2) {
//      System.err.println("Usage: wordcount <in> [<in>...] <out>");
//      System.exit(2);
//    }
        Properties properties = System.getProperties();
        properties.setProperty("HADOOP_USER_NAME", "root");

        String outputPath="/output/";
        // 指定HDFS路径的方法一
        conf.set("fs.defaultFS", "hdfs://172.18.0.2:9000");

        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCountHdfs.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        // for (int i = 0; i < otherArgs.length - 1; ++i) {
        FileInputFormat.addInputPath(job, new Path("/data"));
        // }
        FileSystem fileSystem = FileSystem.get(conf);
        if(fileSystem.exists(new Path(outputPath)))
        {
            fileSystem.delete(new Path(outputPath), true);
        }


        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        job.waitForCompletion(true);





        //查看程序的运行结果
        FSDataInputStream fr = fileSystem.open(new Path(outputPath+"part-r-00000"));
        IOUtils.copyBytes(fr,System.out,1024,true);

    }

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        @Override
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);
            }
        }
    }

    public static class IntSumReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }
}
