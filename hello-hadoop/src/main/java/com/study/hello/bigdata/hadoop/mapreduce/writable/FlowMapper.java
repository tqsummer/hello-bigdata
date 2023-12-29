package com.study.hello.bigdata.hadoop.mapreduce.writable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author : fangxiangqian
 * @created : 12/22/2023
 **/
public class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
    FlowBean flowBean = new FlowBean();

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, FlowBean>.Context context) throws IOException, InterruptedException {

        // 1. 获取一行
        String line = value.toString();

        // 2. 切割
        String[] words = line.split("\t");

        // 3. 输出

        flowBean.setUpFlow(Long.parseLong(words[words.length - 3]));
        flowBean.setDownFlow(Long.parseLong(words[words.length - 2]));
        flowBean.setSumFlow();

        context.write(new Text(words[1]), flowBean);
    }
}
