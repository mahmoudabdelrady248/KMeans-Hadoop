import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class InitializerReducer extends Reducer<IntWritable, Text, IntWritable, Text>{

	@Override
	public void reduce(IntWritable key, Iterable<Text>values,Context context) throws IOException, InterruptedException {
		int i=0; 
		for(Text value :values) context.write(new IntWritable(i++),value);
	}
}
