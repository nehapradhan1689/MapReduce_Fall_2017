package hw3.topk;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import hw3.datastructures.Page;

//TopKReducer takes the output of the Mapper tasks as input and emits local 100
//winners as output. The records are sorted during the shuffle phase based on 
//the criteria mentioned in the secondary sort. There is only one reducer 
//configured and hence all records are handled by the same reducer thus allowing
//for top 100 to be evaluated.
public class TopKReducer extends Reducer<DoubleWritable, Page, Text, NullWritable>{
	
	List<Page> globalTopKDS; 
	Double topKValue;
	
	@Override
	protected void setup(Reducer<DoubleWritable, Page, Text, NullWritable>.Context context)
			throws IOException, InterruptedException {
		globalTopKDS = new ArrayList<Page>();
		Configuration conf = context.getConfiguration();
		topKValue = Double.parseDouble(conf.get("kForTopK"));
		
	}

	@Override
	protected void reduce(DoubleWritable pageRank, Iterable<Page> pages,
			Reducer<DoubleWritable, Page, Text, NullWritable>.Context context) 
					throws IOException, InterruptedException {

//		globalTopKDS = new TreeMap<Double, Page>();
//		
		for(Page page : pages) {
			Page topPage = new Page(new Text(page.getPageName()),new DoubleWritable(page.getPageRank().get()),
					new IntWritable(page.getNoOfOutlinks().get()), page.getOutlinks());
			if(globalTopKDS.size() < topKValue) {
				globalTopKDS.add(topPage);
			}
			else {
				break;
			}
		}
	}
	
	@Override
	protected void cleanup(Reducer<DoubleWritable, Page, Text, NullWritable>.Context context)
			throws IOException, InterruptedException {
		for(Page page : globalTopKDS) {
			context.write(new Text(page.toStringPageRank()), NullWritable.get());
		}
	}
		
		
	}
