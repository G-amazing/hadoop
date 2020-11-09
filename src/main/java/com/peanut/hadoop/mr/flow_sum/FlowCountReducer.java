package com.peanut.hadoop.mr.flow_sum;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowCountReducer extends Reducer<Text, FlowBean, Text, FlowBean> {

    FlowBean v = new FlowBean();

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        long sum_upFlow = 0;
        long sum_downFlow = 0;

        for (FlowBean o : values) {
            sum_upFlow += o.getUpFlow();
            sum_downFlow += o.getDownFlow();
        }

        v.setUpFlow(sum_upFlow);
        v.setDownFlow(sum_downFlow);
        v.setSumFlow(sum_upFlow + sum_downFlow);

        context.write(key, v);
    }
}
