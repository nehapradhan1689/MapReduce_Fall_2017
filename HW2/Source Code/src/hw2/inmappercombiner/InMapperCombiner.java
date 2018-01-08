package hw2.inmappercombiner;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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

//This is the driver class. There is an in mapper combiner class defined.
public class InMapperCombiner {
	
	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("Usage: hadoop jar <JarFile> <JobName> <InputDirectory> <OutputDirectory>");
			System.exit(2);
		}
		Job job = Job.getInstance(conf, "In Mapper Combiner");
		job.setJarByClass(InMapperCombiner.class);
		job.setMapperClass(ClimateDataInMapperCombinerMapper.class);
		job.setReducerClass(ClimateDataInMapperCombinerReducer.class);
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

class ClimateDataInMapperCombinerMapper extends Mapper<LongWritable, Text, Text, StationStatsCombiner> {

	Map<Text, StationStatsCombiner> inMapperCombinerMap;

	//Setup function initializes the hashmap data structure that stores the 
	//intermediate data produced by map calls
	public void setup(Context context) {
		inMapperCombinerMap = new HashMap<>();
	}

	//Map call that populates the data structure initialized in the setup phase
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {

		String line = value.toString();
		String[] recordSplit = line.split(",");
		String obsvType = recordSplit[2].trim();
		Text stationId = new Text(recordSplit[0].trim());
		double temperature = Double.parseDouble(recordSplit[3]);

		if (obsvType.equalsIgnoreCase("TMAX")) {
			if (!inMapperCombinerMap.containsKey(stationId)) {
				inMapperCombinerMap.put(stationId, new StationStatsCombiner());
			}
			StationStatsCombiner stnCom = inMapperCombinerMap.get(stationId);
			stnCom.settMaxSum(new DoubleWritable(stnCom.gettMaxSum().get() + temperature));
			stnCom.settMaxCount(new DoubleWritable(stnCom.gettMaxCount().get() + 1));
			inMapperCombinerMap.put(stationId,stnCom);
		}
		else if (obsvType.equalsIgnoreCase("TMIN")) {
			if (!inMapperCombinerMap.containsKey(stationId)) {
				inMapperCombinerMap.put(stationId, new StationStatsCombiner());
			}
			StationStatsCombiner stnCom = inMapperCombinerMap.get(stationId);
			stnCom.settMinSum(new DoubleWritable(stnCom.gettMinSum().get() + temperature));
			stnCom.settMinCount(new DoubleWritable(stnCom.gettMinCount().get() + 1));
			inMapperCombinerMap.put(stationId,stnCom);
		}
	}
	
	//Cleanup phase that emits a key value pair which combines the map output that
	//have the same key
	public void cleanup(Context context) throws IOException, InterruptedException {
		for(Text stnId : inMapperCombinerMap.keySet()) {
			context.write(stnId, inMapperCombinerMap.get(stnId));
		}
	}
}

//Reducer that combines all the records emitted by map.
class ClimateDataInMapperCombinerReducer extends Reducer<Text, StationStatsCombiner, Text, NullWritable> {

	
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
