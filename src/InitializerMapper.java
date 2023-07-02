import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class InitializerMapper extends Mapper<LongWritable,Text,IntWritable,Text>{
	
	@Override
	public void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException{
		int dim=Integer.valueOf(context.getConfiguration().get("dim"));
		String[] centroid=value.toString().split(",");
		StringBuilder sb=new StringBuilder();
		for(int i=0;i<dim;i++) {
			sb.append(centroid[i]);
			if(i!=dim-1) sb.append(",");
		}
		context.write(new IntWritable(1),new Text(sb.toString()));			
	}

	@Override
	public void run(Mapper<LongWritable,Text,IntWritable,Text>.Context context) throws IOException, InterruptedException{
		int k = Integer.valueOf(context.getConfiguration().get("k"));
		for(int i=0;i<k;i++){
			if(!context.nextKeyValue()) break;
			map(context.getCurrentKey(),context.getCurrentValue(),context);
		}
			
	}
	
}
