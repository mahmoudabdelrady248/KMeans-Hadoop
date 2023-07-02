

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class IterativeReducer extends Reducer<IntWritable, Text, IntWritable, Text>{

	@Override
	public void reduce(IntWritable key,Iterable<Text> values,Context context) throws IOException, InterruptedException {
		Configuration conf=context.getConfiguration();
		int dim=Integer.valueOf(conf.get("dim")),length=0;
		double[] centroid=new double[dim];
		for(Text value:values) {
			String[] point=value.toString().split(",");
			for(int i=0;i<dim;i++) 
				centroid[i]+=Double.valueOf(point[i]);
			length++;
		}
		StringBuilder sb=new StringBuilder() ;
		for(int i=0;i<dim;i++) {
			sb.append(String.valueOf(centroid[i]/length));
			if(i!=dim-1) sb.append(",");
		}
		context.write(key,new Text(sb.toString()));
	}

	
	
}
