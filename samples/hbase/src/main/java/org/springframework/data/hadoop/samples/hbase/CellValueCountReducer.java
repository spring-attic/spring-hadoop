package org.springframework.data.hadoop.samples.hbase;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;


public class CellValueCountReducer extends
		TableReducer<Text, IntWritable, ImmutableBytesWritable> {

	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		int i = 0;
		for (IntWritable val : values) {
			i += val.get();
		}
		Put put = new Put(Bytes.toBytes(key.toString()));
		put.add(Bytes.toBytes("cf"), Bytes.toBytes("count"),
				Bytes.toBytes("tatal is:" + i));
		context.write(null, put);
	}

}
