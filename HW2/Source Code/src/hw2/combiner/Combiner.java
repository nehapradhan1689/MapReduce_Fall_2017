/**
 * 
 */
package hw2.combiner;

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

import hw2.dataStructure.StationStatsCombiner;

//This is the driver class. It has specification for a combiner class that is defined.
public class Combiner {
	
	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("Usage: hadoop jar <JarFile> <JobName> <InputDirectory> <OutputDirectory>");
			System.exit(2);
		}
		Job job = Job.getInstance(conf, "Combiner");
		job.setJarByClass(Combiner.class);
		job.setMapperClass(ClimateDataCombinerMapper.class);
		job.setReducerClass(ClimateDataCombinerReducer.class);
		job.setCombinerClass(ClimateDataCombiner.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(StationStatsCombiner.class);
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
//custom object as value. There is a combiner defined for the map phase which accumulates
//all all records with the same key and therefore provides a pre-processed input to the reducer.
class ClimateDataCombinerMapper extends Mapper<LongWritable, Text, Text, StationStatsCombiner> {
	
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {
		String line = value.toString();
		String[] recordSplit = line.split(",");
		String obsvType = recordSplit[2].trim();
		Text stationId = new Text(recordSplit[0].trim());
		double temperature = Double.parseDouble(recordSplit[3]);
		StationStatsCombiner stnCom = null;
		if (obsvType.equalsIgnoreCase("TMAX")) {
			stnCom = new StationStatsCombiner(temperature, 1.0, 0.0, 0.0);
			context.write(stationId, stnCom);
		}
		else if (obsvType.equalsIgnoreCase("TMIN")) {
			stnCom = new StationStatsCombiner(0.0, 0.0, temperature, 1.0);
			context.write(stationId, stnCom);
		}
	}
}

//Reducer class in which reduce call emits a record that has a station id, tmin and tmax  
//values as key and null as value so as to output records in required format. It is to be noted 
//that this reducer receives values from mappers whose output is combined. 
class ClimateDataCombinerReducer extends Reducer<Text, StationStatsCombiner, Text, NullWritable> {

	public void reduce(Text stationId, Iterable<StationStatsCombiner> stations, Context context) 
			throws IOException, InterruptedException {
		
		double tMaxTempSum = 0.0;
		double tMinTempSum = 0.0;
		double tMaxCount = 0.0;
		double tMinCount = 0.0;
		double meanTMax = 0.0;
		double meanTMin = 0.0;
		
		for (StationStatsCombiner stn : stations) {
			
			tMaxTempSum += stn.gettMaxSum().get();
			tMaxCount += stn.gettMaxCount().get();
			tMinTempSum += stn.gettMinSum().get();
			tMinCount += stn.gettMinCount().get();
			
		}
//		System.out.println("TMAX sum in Reducer: " + tMaxTempSum);
//
//		System.out.println("TMIN sum in Reducer: " + tMinTempSum);
//
//		System.out.println("TMAXCOUNT sum in Reducer: " + tMaxCount);
//
//		System.out.println("TMINCOUNT sum in Reducer: " + tMinCount);

		if(tMaxCount != 0) {
			meanTMax = tMaxTempSum / tMaxCount;
		}
		if(tMinCount != 0) {
			meanTMin = tMinTempSum / tMinCount;
		}
		
		context.write(new Text(stationId + ", " + meanTMin + ", " + meanTMax), NullWritable.get());
	}
}

//Combiner class that accumulates data with the same station id so as to provide the reducer with a
//combined output from a mapper. 
class ClimateDataCombiner extends Reducer<Text, StationStatsCombiner, Text, StationStatsCombiner> {
	
	public void reduce(Text stationId, Iterable<StationStatsCombiner> stations, Context context) 
			throws IOException, InterruptedException {
		
		double tMaxTempSum = 0.0;
		double tMaxCount = 0.0;
		double tMinTempSum = 0.0;
		double tMinCount = 0.0;
		
		for (StationStatsCombiner stn : stations) {
			
//			System.out.println("Temperature in Reducer: " + temp);
			if (stn.gettMaxCount().get() != 0.0) {
				tMaxTempSum += stn.gettMaxSum().get();
				tMaxCount++;
			}
			else {
				tMinTempSum += stn.gettMinSum().get();
				tMinCount++;

			}
		}		
		
		context.write(stationId, new StationStatsCombiner(tMaxTempSum, tMaxCount, tMinTempSum, tMinCount));
	}
}
