package hw2.noCombiner;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import hw2.dataStructure.StationStats;

//This is the driver class. There is no combiner class defined for this program.
public class NoCombiner {

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("Usage: hadoop jar <JarFile> <JobName> <InputDirectory> <OutputDirectory>");
			System.exit(2);
		}
		Job job = Job.getInstance(conf, "No Combiner");
		job.setJarByClass(NoCombiner.class);
		job.setMapperClass(ClimateDataNoCombinerMapper.class);
		job.setReducerClass(ClimateDataNoCombinerReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(StationStats.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		for (int i = 0; i < otherArgs.length - 1; ++i) {
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}
		FileOutputFormat.setOutputPath(job,
				new Path(otherArgs[otherArgs.length - 1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}

//Mapper class in which map call emits a record that has a station id as key and a 
//custom object as value. There is no combiner defined for the map phase and hence 
//all accumulation happens in the reduce phase.
class ClimateDataNoCombinerMapper extends Mapper<LongWritable, Text, Text, StationStats> {
	
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {
		String line = value.toString();
		String[] recordSplit = line.split(",");
		String obsvType = recordSplit[2].trim();
		if (obsvType.equalsIgnoreCase("TMAX") || obsvType.equalsIgnoreCase("TMIN")) {
			Text stationId = new Text(recordSplit[0].trim());
			double temperature = Double.parseDouble(recordSplit[3]);
			StationStats stn = new StationStats(obsvType, temperature);
			context.write(stationId, stn);

		}
	}
}

//Reducer class in which reduce call emits a record that has a station id, tmin and tmax  
//values as key and null as value so as to output records in required format. 
class ClimateDataNoCombinerReducer extends Reducer<Text, StationStats, Text, NullWritable> {

	public void reduce(Text stationId, Iterable<StationStats> stations, Context context) 
			throws IOException, InterruptedException {
		
		double tMaxTempSum = 0;
		double tMinTempSum = 0;
		double tMaxCount = 0;
		double tMinCount = 0;
		double meanTMax = 0;
		double meanTMin = 0;
		
		//Calculating tmin and tmax for each station id
		for (StationStats stn : stations) {
			String obsType = stn.getObsvType().toString();
			double temp = stn.getTemperature().get();
//			System.out.println("Temperature in Reducer: " + temp);
			if (obsType.equalsIgnoreCase("TMAX")) {
				tMaxTempSum += temp;
				tMaxCount++;
			}
			else if (obsType.equalsIgnoreCase("TMIN")) {
				tMinTempSum += temp;
				tMinCount++;

			}
		}
//		System.out.println("TMAX sum in Reducer: " + tMaxTempSum);
//
//		System.out.println("TMIN sum in Reducer: " + tMinTempSum);
//
//		System.out.println("TMAXCOUNT sum in Reducer: " + tMaxCount);
//
//		System.out.println("TMINCOUNT sum in Reducer: " + tMinCount);

		//Getting average tmin and tmax
		if(tMaxCount != 0) {
			meanTMax = tMaxTempSum / tMaxCount;
		}
		if(tMinCount != 0) {
			meanTMin = tMinTempSum / tMinCount;
		}
		
		context.write(new Text(stationId + ", " + meanTMin + ", " + meanTMax), NullWritable.get());
	}
}
