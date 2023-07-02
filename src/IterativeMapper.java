import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class IterativeMapper extends Mapper<LongWritable, Text, IntWritable, Text>{

	@Override
	public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException {
		Configuration conf=context.getConfiguration();
		String[] point=value.toString().split(",");
		if(point.length-1!=0){
			int k=Integer.valueOf(conf.get("k")),dim=Integer.valueOf(conf.get("dim"));
			double minDistance=Double.MAX_VALUE;int index=-1;
			for (int i=0;i<k;i++){
				String[] centroid=conf.get("centroid"+Integer.toString(i)).split(",");
				double distance=0;
				for(int j=0;j<dim;j++) 
					distance+=Math.pow(Double.parseDouble(point[j])-Double.parseDouble(centroid[j]),2); 
				if(distance<minDistance) {minDistance=distance;index=i;}
			}
			context.write(new IntWritable(index),value);
		}
	}
}
