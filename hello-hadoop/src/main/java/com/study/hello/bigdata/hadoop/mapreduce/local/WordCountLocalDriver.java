package com.study.hello.bigdata.hadoop.mapreduce.local;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author : fangxiangqian
 * @created : 12/22/2023
 **/
public class WordCountLocalDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // 1. 获取配置信息，或者 job 对象实例
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);
        // 2. 指定本程序的 jar 包所在的本地路径
        job.setJarByClass(WordCountLocalDriver.class);
        // 3. 指定本业务 job 要使用的 mapper/reducer 业务类
        job.setMapperClass(WordCountLocalMapper.class);
        job.setReducerClass(WordCountLocalReducer.class);
        // 4. 指定 mapper 输出数据的 kv 类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        // 5. 指定最终输出的数据的 kv 类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        // 6. 指定 job 的输入原始文件所在目录
        //FileInputFormat.setInputPaths(job, new Path("/input"));
        FileInputFormat.setInputPaths(job, new Path("D:\\Workspace\\DataWorkspace\\Hadoop\\input"));
        // 7. 指定 job 的输出结果所在目录
        //FileOutputFormat.setOutputPath(job, new Path("/output"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\Workspace\\DataWorkspace\\Hadoop\\output"));
        // 8. 提交作业
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);

    }
}
