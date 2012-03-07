package org.springframework.data.hadoop.samples.hbase;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class CellValueHDFSReducer extends
		Reducer<Text, IntWritable, Text, IntWritable> {

	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		int i = 0;
		for (IntWritable v : values) {
			i += v.get();
		}
		context.write(key, new IntWritable(i));

	}

}
