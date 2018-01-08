package hw2.secondarysort;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import hw2.comparators.GroupingComparator;
import hw2.comparators.HashPartitioner;
import hw2.comparators.KeyComparator;
import hw2.dataStructure.CompositeKey;
import hw2.dataStructure.StationStatsCombiner;

public class SecondarySort {
	
	//Driver class that defines comparators and partitioner used for secondary sort
	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("Usage: hadoop jar <JarFile> <JobName> <InputDirectory> <OutputDirectory>");
			System.exit(2);
		}
		Job job = Job.getInstance(conf, "Secondary Sort");
		job.setJarByClass(SecondarySort.class);
		job.setMapperClass(ClimateDataSecondarySortMapper.class);
		job.setReducerClass(ClimateDataSecondarySortReducer.class);
		job.setSortComparatorClass(KeyComparator.class);
		job.setPartitionerClass(HashPartitioner.class);
		job.setGroupingComparatorClass(GroupingComparator.class);
		job.setMapOutputKeyClass(CompositeKey.class);
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

//Secondary sort class that emits a record containing a composite key and a value that is defined as a custom
//datatype
class ClimateDataSecondarySortMapper extends Mapper<LongWritable, Text, CompositeKey, StationStatsCombiner> {
	
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {
		
		String line = value.toString();
		String[] recordSplit = line.split(",");
		String obsvType = recordSplit[2].trim();
		Text stationId = new Text(recordSplit[0].trim());
		int year = Integer.parseInt(recordSplit[1].substring(0, 4));
		double temperature = Double.parseDouble(recordSplit[3]);
		StationStatsCombiner stnCom = null;
		CompositeKey compKey = new CompositeKey(stationId, new IntWritable(year));
		if (obsvType.equalsIgnoreCase("TMAX")) {
			stnCom = new StationStatsCombiner(temperature, 1.0, 0.0, 0.0);
			context.write(compKey, stnCom);
		}
		else if (obsvType.equalsIgnoreCase("TMIN")) {
			stnCom = new StationStatsCombiner(0.0, 0.0, temperature, 1.0);
			context.write(compKey, stnCom);
		}
	
	}
}

//Reducer class that combines the results of the map function to produce records in a manner that is 
//sorted by the year in which the mean temperatures were recorded
class ClimateDataSecondarySortReducer extends Reducer<CompositeKey, StationStatsCombiner, Text, NullWritable> {

	
	public void reduce(CompositeKey compKey, Iterable<StationStatsCombiner> stations, Context context) 
			throws IOException, InterruptedException {
		
		double tMaxTempSum = 0.0;
		double tMinTempSum = 0.0;
		double tMaxCount = 0.0;
		double tMinCount = 0.0;
		double meanTMax = 0.0;
		double meanTMin = 0.0;
		
		int thisYear = compKey.getYear().get();
		int lastYear = compKey.getYear().get();
		
		//Initializing the format in which output was required
		StringBuilder outputRecord = new StringBuilder();
		outputRecord.append(compKey.getStationID());
		outputRecord.append(", [");
		
		for (StationStatsCombiner stn : stations) {
			
			lastYear = thisYear;
			thisYear = compKey.getYear().get();
			
			//If year is changing then make an entry of the accumulated tmin and tmax 
			//details of the current year's records
			if(lastYear != thisYear) {
				outputRecord.append("(");
				outputRecord.append(lastYear);
				outputRecord.append(", ");
				
				if(tMaxCount != 0) {
					meanTMax = tMaxTempSum / tMaxCount;
				}
				if(tMinCount != 0) {
					meanTMin = tMinTempSum / tMinCount;
				}
				
				outputRecord.append(meanTMin);
				outputRecord.append(", ");
				outputRecord.append(meanTMax);
				outputRecord.append("), ");
				
				tMaxTempSum = 0.0;
				tMinTempSum = 0.0;
				tMaxCount = 0.0;
				tMinCount = 0.0;
				meanTMax = 0.0;
				meanTMin = 0.0;
				
			}
			
			//Accumulate the weather data for the current year until
			//the year doesn't change
			tMaxTempSum += stn.gettMaxSum().get();
			tMaxCount += stn.gettMaxCount().get();
			tMinTempSum += stn.gettMinSum().get();
			tMinCount += stn.gettMinCount().get();
			
		}
		
		//For the last record of the year, because that is not included
		if(lastYear == thisYear) {
			
			outputRecord.append("(");
			outputRecord.append(lastYear);
			outputRecord.append(", ");
			
			if(tMaxCount != 0) {
				meanTMax = tMaxTempSum / tMaxCount;
			}
			if(tMinCount != 0) {
				meanTMin = tMinTempSum / tMinCount;
			}
			
			outputRecord.append(meanTMin);
			outputRecord.append(", ");
			outputRecord.append(meanTMax);
			outputRecord.append(")");
			
		}
		
		outputRecord.append("]");
		context.write(new Text(outputRecord.toString()), NullWritable.get());
	}
}


