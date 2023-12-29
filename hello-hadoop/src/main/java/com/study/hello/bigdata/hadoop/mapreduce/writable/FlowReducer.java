package com.study.hello.bigdata.hadoop.mapreduce.writable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author : fangxiangqian
 * @created : 12/22/2023
 **/
public class FlowReducer extends Reducer<Text, FlowBean, Text, FlowBean> {
    private FlowBean flowBean = new FlowBean();

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Reducer<Text, FlowBean, Text, FlowBean>.Context context) throws IOException, InterruptedException {
        long sumUpFlow = 0;
        long sumDownFlow = 0;

        for (FlowBean value : values) {
            sumUpFlow += value.getUpFlow();
            sumDownFlow += value.getDownFlow();
        }

        flowBean.setUpFlow(sumUpFlow);
        flowBean.setDownFlow(sumDownFlow);
        flowBean.setSumFlow();

        context.write(key, flowBean);
    }
}
