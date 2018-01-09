package hw3.preprocessor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import hw3.datastructures.Page;


//Combiner class that accumulates similar pages and ensures that the reducer
//receives a concise combined output from the mapper
public class PreprocessorCombiner extends Reducer<Page, Page, Page, Page> {

	public void reduce(Page pageKey, Iterable<Page> pages, Context context) 
			throws IOException, InterruptedException {

		int outlinksSize = 0;
		List<String> combinedOutlinks = new ArrayList<String>();

		for(Page page : pages) {
			outlinksSize += page.getNoOfOutlinks().get();
			combinedOutlinks.addAll(page.getOutlinks());
		}

		Page newPage = new Page(pageKey.getPageName(), new IntWritable(outlinksSize), 
				combinedOutlinks);
		context.write(newPage, newPage);

	}
}

