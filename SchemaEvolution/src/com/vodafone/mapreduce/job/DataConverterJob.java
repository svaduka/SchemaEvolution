package com.vodafone.mapreduce.job;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.vodafone.mapreduce.mapper.DataConverterMapper;
import com.vodafone.mapreduce.reducer.DataConverterReducer;

public class DataConverterJob extends Configured implements Tool {

	public static void main(String[] args) throws IOException {

		Configuration conf = new Configuration(Boolean.TRUE);
		
		try{
			int i = ToolRunner.run(conf, new DataConverterJob(), args);
			if(i==0){
				System.out.println("SUCCESS");
			}else{
				System.out.println("FAILED");
			}
		}catch(Exception e){
			e.printStackTrace();
		}
				
	}
	
	@Override
	public int run(String[] args) throws Exception {
		
		Configuration conf = super.getConf();
		Job dataConverterJob = Job.getInstance(conf, "DataConverterJob");
		dataConverterJob.setJarByClass(DataConverterJob.class);

		FileInputFormat.addInputPath(dataConverterJob, new Path(args[0]));
		FileOutputFormat.setOutputPath(dataConverterJob, new Path(args[1]));
		
		
		dataConverterJob.setMapperClass(DataConverterMapper.class);
		dataConverterJob.setReducerClass(DataConverterReducer.class);
		
		return dataConverterJob.waitForCompletion(Boolean.TRUE)?0:-1;
	}

}
