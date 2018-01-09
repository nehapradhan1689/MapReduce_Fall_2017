package hw3.topk;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import hw3.datastructures.Page;

//TopKMapper takes the output of the page rank job as input and emits local 100
//winners as output. These are secondary sorted in descending order of page
//ranks.
public class TopKMapper extends Mapper<LongWritable, Text, DoubleWritable, Page>{

	Map<Page,Double> localTopKDS; 
	Integer topKValue;

	@Override
	protected void setup(Mapper<LongWritable, Text, DoubleWritable, Page>.Context context)
			throws IOException, InterruptedException {

		//Getting the configuration set up 
		Configuration conf = context.getConfiguration();
		topKValue = Integer.parseInt(conf.get("kForTopK"));
		localTopKDS = new HashMap<Page,Double>();

	}

	@Override
	protected void map(LongWritable key, Text value, 
			Mapper<LongWritable, Text, DoubleWritable, Page>.Context context)
					throws IOException, InterruptedException {

		//Getting details from input line to form a Page structure
		String[] lineSplit = value.toString().split("~");
		Text pageName = new Text(lineSplit[0]);
		Double pageRank = Double.parseDouble(lineSplit[1]);
		Integer noOfOutlinks = Integer.parseInt(lineSplit[2]);
		List<String> outlinks = new ArrayList<String>();
		if(lineSplit.length > 3) {
			String[] outlinksSplit = lineSplit[3].split("->");
			for(String link: outlinksSplit) {
				outlinks.add(link);
			}
		}
		
		Page page = new Page(pageName, new DoubleWritable(pageRank), 
				new IntWritable(noOfOutlinks), outlinks);
		
		localTopKDS.put(page, pageRank);
		
	}

	@Override
	protected void cleanup(Mapper<LongWritable, Text, DoubleWritable, Page>.Context context)
			throws IOException, InterruptedException {
		
//		Map<Page,Double> mapDSSorted = SortMapByValuesUtil.sortbyValue(localTopKDS);
		List<Map.Entry<Page,Double>> listOfMapValues = 
				new LinkedList<Map.Entry<Page,Double>>(localTopKDS.entrySet());
		Collections.sort(listOfMapValues, new Comparator<Map.Entry<Page,Double>>() {

			@Override
			public int compare(Map.Entry<Page, Double> o1, Map.Entry<Page, Double> o2) {
				return ((o2.getValue()).compareTo(o1.getValue()));
			}
		});
		
		Map<Page,Double> mapDSSorted = new LinkedHashMap<Page,Double>();
		for(Map.Entry<Page,Double> entry : listOfMapValues) {
			mapDSSorted.put(entry.getKey(),entry.getValue());
		}
		
		int noOfEmissions = Math.min(mapDSSorted.size(), topKValue);
		int counter = 0;
		
		for(Page key : mapDSSorted.keySet()) {
			
			if(counter < noOfEmissions) {
				context.write(new DoubleWritable(mapDSSorted.get(key)), key);
			}
			else {
				break;
			}
			counter++;
		}

	}
	

}
