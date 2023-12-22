package com.study.hello.bigdata.hadoop.mapreduce.local;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author : fangxiangqian
 * @created : 12/22/2023
 **/
public class WordCountLocalReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable intWritable = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {

        // 1. 累加求和
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }

        intWritable.set(sum);

        // 2. 输出
        context.write(key, intWritable);
    }
}
