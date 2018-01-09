package hw3.pagerank;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

import hw3.counters.DanglingCounter;
import hw3.datastructures.EmitMultiValueWritable;
import hw3.datastructures.Page;

//Reducer receives pageName as key and multi-valued iterable as value. If the incoming value is
//a double value then it goes on to contribute to the page rank of the the key, if it is a 
//Page then it is stored as the the current page for which the page rank value is being
//evaluated and the page rank computed will be set before the reduce phase done.
public class PageRankReducer extends Reducer<Text, EmitMultiValueWritable, Text, NullWritable>{

	private final Long doubleToLongConversionFactor = 1000000000000L;
	private Double danglingNodeMass;
	
	@Override
	protected void reduce(Text pageName, Iterable<EmitMultiValueWritable> pageOrRanks,
			Reducer<Text, EmitMultiValueWritable, Text, NullWritable>.Context context)
					throws IOException, InterruptedException {

		//Get configuration set in Driver from context in order to calculate 
		//dangling factor
		Configuration conf = context.getConfiguration();
		Long totalPages = conf.getLong("noOfPages", 0L);
		danglingNodeMass = Double.valueOf(conf.getLong("danglingPageRank", 0L));
		danglingNodeMass /= doubleToLongConversionFactor;
//		Double danglingNodeMassForNextIteration = conf.getDouble("danglingPageRank", 0.0);
		Double sumOfIncomingRanks = 0.0;
		Double alphaValue = 0.15;
		Double reverseAlpha = 0.85;
		Page page = null;

		//Check for multivalue types. If it is a Page then set it as the current
		//page for which we are calculating the page rank. If it is a page rank
		//value, sum its value to calculate the page rank for the page
		for(EmitMultiValueWritable obj : pageOrRanks) {
			Writable wObj = obj.get();
			if(wObj instanceof Page) {
				page = (Page) wObj;
			}
			else if(wObj instanceof DoubleWritable) {
				sumOfIncomingRanks += ((DoubleWritable) wObj).get();
			}
		}
		
		//Page rank evaluation
//		Double prevPageRank = page.getPageRank().get();
		Double currentPageRank = (alphaValue/totalPages.doubleValue()) + 
				(reverseAlpha * ((danglingNodeMass/totalPages.doubleValue()) + 
						sumOfIncomingRanks));
		
//		if(page.getNoOfOutlinks().get() == 0) {
//			Long danglingNodeMassSoFar = context.getCounter(DanglingCounter.DANGLING_NODE_MASS).getValue();
//			Double danglingPageRankMassSoFar = Double.longBitsToDouble(danglingNodeMassSoFar);
//			danglingPageRankMassSoFar += prevPageRank;
//			Long danglingNodeMassForNextIteration = Double.doubleToLongBits(danglingPageRankMassSoFar);
//			
//			context.getCounter(DanglingCounter.DANGLING_NODE_MASS).setValue(danglingNodeMassForNextIteration);
//		}

		Long runningPageRankTotal = context.getCounter(DanglingCounter.TOTAL_PAGE_RANK).getValue();
		Double runningPageRankTotalDouble = Double.longBitsToDouble(runningPageRankTotal);
		runningPageRankTotalDouble += currentPageRank;
		Long pageRankTotal = Double.doubleToLongBits(runningPageRankTotalDouble);
		
		context.getCounter(DanglingCounter.TOTAL_PAGE_RANK).setValue(pageRankTotal);
		
		page.setPageRank(new DoubleWritable(currentPageRank));

		context.write(new Text(page.toString()), NullWritable.get());

	}
}
