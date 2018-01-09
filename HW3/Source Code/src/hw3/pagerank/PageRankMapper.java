package hw3.pagerank;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import hw3.counters.DanglingCounter;
import hw3.datastructures.EmitMultiValueWritable;
import hw3.datastructures.Page;

//Mapper receives some offset value as key and a line from the input file as
//value. A page structure is created from the information obtained from the 
//input file. Page ranks are distributed evenly among outlinks.
public class PageRankMapper extends Mapper<LongWritable, Text, Text, EmitMultiValueWritable> {
	
	private final Long doubleToLongConversionFactor = 1000000000000L;
	
	@Override
	protected void map(LongWritable key, Text value, 
			Mapper<LongWritable, Text, Text, EmitMultiValueWritable>.Context context)
			throws IOException, InterruptedException {
		
		//Getting configuration set in Driver from context 
		Configuration conf = context.getConfiguration();
		Long totalPages = conf.getLong("noOfPages", 0L);
		boolean isFirstPageRankIteration = conf.getBoolean("isFirstPageRankIteration", true);
		
		//Getting details from input line to form a Page structure
		String[] lineSplit = value.toString().split("~");
		Text pageName = new Text(lineSplit[0]);
		Double currentPageRank;
		Integer noOfOutlinks = Integer.parseInt(lineSplit[2]);
		List<String> outlinks = new ArrayList<String>();
		if(lineSplit.length > 3) {
			String[] outlinksSplit = lineSplit[3].split("->");
			for(String link: outlinksSplit) {
				outlinks.add(link);
			}
		}
		
		//If this is the first iteration of Page Rank evaluation, then supply 
		//equal weights to all pages in the data set, if not supply the current
		//page's rank over all its outlinks
		if(isFirstPageRankIteration) {
			currentPageRank = 1.0/totalPages.doubleValue();
		}
		else {
			currentPageRank = Double.parseDouble(lineSplit[1]);
		}
		
		Page currentPage = new Page(pageName, new DoubleWritable(currentPageRank), 
				new IntWritable(noOfOutlinks), outlinks);
		context.write(pageName, new EmitMultiValueWritable(currentPage));
		
		//If the constructed page has outlinks supply currentPage pageRank over
		//all its outlinks with equal weight given to each outlink. If there 
		//no outlinks, then collect the page rank to be maintained as a factor
		//to handle dangling nodes
		if(noOfOutlinks != 0) {
			Double outlinkPageRankWeight = currentPageRank/noOfOutlinks.doubleValue();
			for(String oLink : outlinks) {	
				context.write(new Text(oLink), new EmitMultiValueWritable
						(new DoubleWritable(outlinkPageRankWeight)));
			}
		}
		else {
			/*Long danglingNodeMassSoFar = context.getCounter(DanglingCounter.DANGLING_NODE_MASS).getValue();
			Double danglingPageRankMassSoFar = Double.longBitsToDouble(danglingNodeMassSoFar);
			danglingPageRankMassSoFar += currentPageRank;
			Long danglingNodeMass = Double.doubleToLongBits(danglingPageRankMassSoFar);*/
			
//			Long danglingNodeMass = Double.doubleToLongBits(currentPageRank);
			
			context.getCounter(DanglingCounter.DANGLING_NODE_MASS).increment
			((long) (currentPageRank * doubleToLongConversionFactor));
		}
//		context.write(pageName, new EmitMultiValueWritable(currentPage));
	}
}
