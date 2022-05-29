package com.Nitika.hadoop_demo;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MyReducer extends Reducer<	Text,IntWritable, Text, IntWritable>{
@Override
protected void reduce(Text key, Iterable<IntWritable> values,
		Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
	int count=0;
	Iterator<IntWritable> Iterator = values.iterator();
	while(Iterator.hasNext()) {
		count+=Iterator.next().get();
	}
	context.write(key, new IntWritable(count));
}
}
