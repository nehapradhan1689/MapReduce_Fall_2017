package hw3.driver;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import hw3.counters.DanglingCounter;
import hw3.datastructures.EmitMultiValueWritable;
import hw3.datastructures.Page;
import hw3.pagerank.PageRankMapper;
import hw3.pagerank.PageRankReducer;
import hw3.preprocessor.GroupingComparator;
import hw3.preprocessor.HashPartitioner;
import hw3.preprocessor.KeyComparator;
import hw3.preprocessor.PreprocessorCombiner;
import hw3.preprocessor.PreprocessorMapper;
import hw3.preprocessor.PreprocessorReducer;
import hw3.topk.TopKKeyComparator;
import hw3.topk.TopKMapper;
import hw3.topk.TopKReducer;

//The Driver class is the starting point of the jobs that set out to pre-process
//and evaluate page rank and top-k pages. The jobs are carried out one after 
//another with the output of the first job being the input of the second job and
//so on.
public class Driver {
//	Long noOfPages = 0L;

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		String preprocessInput = "";
		for (int i = 0; i < otherArgs.length - 1; ++i) {
			preprocessInput = otherArgs[i];
		}
		String preprocessOutputPath = otherArgs[otherArgs.length - 1];
		if (otherArgs.length < 2) {
			System.err.println("Usage: hadoop jar <JarFile> <JobName> <InputDirectory> <OutputDirectory>");
			System.exit(2);
		}

		//Runs the pre-processing job
		preprocessorJob(conf, preprocessInput, preprocessOutputPath);

		//Setting configuration variables to aid in page rank evaluation
		conf.setBoolean("isFirstPageRankIteration", true);
//		conf.setDouble("danglingPageRank",0.0);

		//Runs the page rank evaluation job
		for(int i = 0; i < 10; i++) {
			pageRankJob(conf, i, preprocessOutputPath);
			conf.setBoolean("isFirstPageRankIteration", false);
		}
		
		//Runs the top-k evaluation job
		topKPages(conf, preprocessOutputPath);

	}

	//Setting up configurations required to  evaluate top-k page ranks
	private static void topKPages(Configuration conf, String path) {
		
		try {
			conf.set("kForTopK", "100");
			Job job;
			job = Job.getInstance(conf, "TopKEvaluation");
			job.setJarByClass(Driver.class);
			job.setMapperClass(TopKMapper.class);
			job.setReducerClass(TopKReducer.class);
			job.setNumReduceTasks(1);
			job.setSortComparatorClass(TopKKeyComparator.class);
			job.setMapOutputKeyClass(DoubleWritable.class);
			job.setMapOutputValueClass(Page.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(NullWritable.class);

			//Setting input and output file paths
			StringBuilder inputPath = new StringBuilder(path + "_10");
			StringBuilder outputPath = new StringBuilder(path + "_TopKResults");
			FileInputFormat.addInputPath(job, new Path(inputPath.toString()));
			FileOutputFormat.setOutputPath(job, new Path(outputPath.toString()));
			
			boolean isThirdJobComplete = job.waitForCompletion(true);
			if(!isThirdJobComplete) {
				throw new Exception("Page Rank failed");
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	//Setting up configurations required to  evaluate page ranks for all pages
	//in the data set
	private static void pageRankJob(Configuration conf, int iterationNo, 
			String initialPath) {

		try {		
			Job job;
			job = Job.getInstance(conf, "PageRankEvaluation");
			job.setJarByClass(Driver.class);
			job.setMapperClass(PageRankMapper.class);
			job.setReducerClass(PageRankReducer.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(EmitMultiValueWritable.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(NullWritable.class);

			StringBuilder inputPath = new StringBuilder(initialPath + "_" + iterationNo);
			StringBuilder outputPath = new StringBuilder(initialPath + "_" + (iterationNo+1));
			FileInputFormat.addInputPath(job, new Path(inputPath.toString()));
			FileOutputFormat.setOutputPath(job, new Path(outputPath.toString()));
			
			boolean isSecondJobComplete = job.waitForCompletion(true);
			if(!isSecondJobComplete) {
				throw new Exception("Page Rank failed");
			}
			
			//Counters configured to the dangling nodes factor in each iteration
			Counters counters = job.getCounters();
			Long danglingPageRankLV = counters.findCounter(DanglingCounter.DANGLING_NODE_MASS).getValue();
//			Double danglingPageRank = Double.longBitsToDouble(danglingPageRankLV);
			conf.setLong("danglingPageRank", danglingPageRankLV);
			
			Long totalPageRankLV = counters.findCounter(DanglingCounter.TOTAL_PAGE_RANK).getValue();
			Double totalPageRankDouble = Double.longBitsToDouble(totalPageRankLV);
			System.out.println("Total page rank in iteration " + iterationNo + "=" + totalPageRankDouble);
			
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	//Setting up configurations required to parse and pre-process the input data
	//required as input to page rank evaluation
	private static void preprocessorJob(Configuration conf, String inputPath, 
			String outputPath) {

		try {
			Job job;
			job = Job.getInstance(conf, "Preprocessor");
			job.setJarByClass(Driver.class);
			job.setMapperClass(PreprocessorMapper.class);
			job.setReducerClass(PreprocessorReducer.class);
			job.setCombinerClass(PreprocessorCombiner.class);
			job.setMapOutputKeyClass(Page.class);
			job.setMapOutputValueClass(Page.class);
			job.setSortComparatorClass(KeyComparator.class);
			job.setPartitionerClass(HashPartitioner.class);
			job.setGroupingComparatorClass(GroupingComparator.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(NullWritable.class);
			
			StringBuilder outputPathPRInput = new StringBuilder(outputPath + "_0");
			FileInputFormat.addInputPath(job, new Path(inputPath));
			FileOutputFormat.setOutputPath(job, new Path(outputPathPRInput.toString()));

			boolean isFirstJobComplete = job.waitForCompletion(true);
			if(!isFirstJobComplete) {
				throw new Exception("Preprocessing failed");
			}

			//Setting the configuration to hold the total number of pages 
			//required to evaluate page rank in the next job
			Long noOfPages = job.getCounters().
					findCounter("org.apache.hadoop.mapred.Task$Counter","REDUCE_OUTPUT_RECORDS")
					.getValue();
			conf.setLong("noOfPages", noOfPages);
			
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
