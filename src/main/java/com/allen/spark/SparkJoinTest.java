package com.allen.spark;

/**
 * @author : allengent@163.com
 * @date : 2019/9/23 18:17
 * description :
 */
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * @author : allengent@163.com
 * @date : 2019/5/14 09:09
 * description :
 */
public class SparkJoinTest {
    public static void main(String[] args){
        SparkConf conf = new SparkConf().setAppName("SparkRDD").setMaster("spark://ubuntu:7077");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> data = Arrays.asList(1,2,3,4,5);
        JavaRDD<Integer> rdd = sc.parallelize(data);

        //FirstRDD
        JavaPairRDD<Integer, Integer> firstRDD = rdd.mapToPair(new PairFunction<Integer, Integer, Integer>() {
            @Override
            public Tuple2<Integer, Integer> call(Integer num) throws Exception {
                return new Tuple2<>(num, num * num);
            }
        });
        System.out.println("print first rdd");
        print(firstRDD);

        //SecondRDD
        JavaPairRDD<Integer, String> secondRDD = rdd.mapToPair(new PairFunction<Integer, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(Integer num) throws Exception {
                return new Tuple2<>(num, String.valueOf((char)(64 + num * num)));
            }
        });

        System.out.println("print second rdd");
        print(secondRDD);

        JavaPairRDD<Integer, Tuple2<Integer, String>> joinRDD = firstRDD.join(secondRDD);

        System.out.println("print join rdd");
        print(joinRDD);

        JavaRDD<String> res = joinRDD.map(new Function<Tuple2<Integer, Tuple2<Integer, String>>, String>() {
            @Override
            public String call(Tuple2<Integer, Tuple2<Integer, String>> integerTuple2Tuple2) throws Exception {
                int key = integerTuple2Tuple2._1();
                int value1 = integerTuple2Tuple2._2()._1();
                String value2 = integerTuple2Tuple2._2()._2();
                return "" + key + ":(" + value1 + "," + value2 + ")";
            }
        });

        List<String> resList = res.collect();
        for(String str : resList)
        {
            System.out.println(str);
        }


        sc.stop();
    }

    public  static void print( JavaPairRDD rdd)
    {
        List resList = rdd.collect();
        resList.stream().forEach( (d)->
        {
            System.out.println(d);

        });


    }

}