

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class KMean {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		if(args.length != 4) {
			System.out.print("args should be 2 : <inputpath> <outpath> <number of clusters> <number of features> .") ;
			System.exit(-1);
		}
		int k=Integer.valueOf(args[2]),dim=Integer.valueOf(args[3]);
		
		Job initJob = Job.getInstance();
		initJob.setJarByClass(KMean.class);
		initJob.setJobName("kmean");
		initJob.setMapperClass(InitializerMapper.class);
		initJob.setReducerClass(InitializerReducer.class);
		initJob.setOutputKeyClass(IntWritable.class);
		initJob.setOutputValueClass(Text.class);
		
		Configuration initConf = initJob.getConfiguration();
		initConf.set("k",args[2]);
		initConf.set("dim",args[3]);
		
		Path outputPath=new Path(args[1]+"_k_"+k+"_"+0);
		FileInputFormat.addInputPath(initJob, new Path(args[0]));
		FileOutputFormat.setOutputPath(initJob, outputPath);
        FileSystem hdfs=FileSystem.get(initConf);
        if (hdfs.exists(outputPath))
          hdfs.delete(outputPath,true);
		
        initJob.waitForCompletion(true);
		
		double[][] oldCentroids=new double[k][dim];
		long t1=System.currentTimeMillis();
		int counter=1;
		while(true) {
			Job job = Job.getInstance();
			job.setJarByClass(KMean.class);
			job.setJobName("kmean");
			job.setMapperClass(IterativeMapper.class);
			job.setReducerClass(IterativeReducer.class);
			job.setOutputKeyClass(IntWritable.class);
			job.setOutputValueClass(Text.class);
			
			Configuration conf = job.getConfiguration();
			conf.set("k",args[2]);
			conf.set("dim",args[3]);
			Path oldPath=new Path(args[1]+"_k_"+k+"_"+(counter-1)+"/part-r-00000");
			BufferedReader input_buffer=new BufferedReader(new InputStreamReader(FileSystem.get(conf).open(oldPath)));
			Path newPath=new Path(args[1]+"_k_"+k+"_"+counter);
			FileInputFormat.addInputPath(job,new Path(args[0]));
			FileOutputFormat.setOutputPath(job,newPath);
			FileSystem hdfs_ = FileSystem.get(conf);
	        if (hdfs_.exists(newPath))
	        	hdfs_.delete(newPath,true);
			
			double distance=0 ;
			double[][] newCentroids=new double[k][dim] ;
			for(int i=0;i<k;i++) {	
				String line=input_buffer.readLine();
				conf.set("centroid"+i,line.split("\t")[1]);
				String[] centroid=line.split("\t")[1].split(",");
				for(int j=0;j<dim;j++) {
					newCentroids[i][j]=Double.valueOf(centroid[j]) ;
					distance+= Math.pow(newCentroids[i][j]-oldCentroids[i][j],2);
				}
			}
			double threshold=0;
			if(distance<=threshold) break;
			
			job.waitForCompletion(true);
			
			oldCentroids=newCentroids;
			counter++;
		}
		long t2=System.currentTimeMillis() ;
		System.out.println("\n Time taken by parallel version : "+(((t2-t1)/1000)/60)+"min");
	}
	
}
