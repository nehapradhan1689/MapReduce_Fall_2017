package hw3.preprocessor;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import hw3.datastructures.Page;

//Performing a secondary sort on the user defined key ensures that out of all the 
//records that the mapper emits, only the record with the the structure containing
//details about the page and its outlinks is present at the top, with the rest of
//the records being ignored
public class PreprocessorReducer extends Reducer<Page, Page, Text, NullWritable> {

	@Override
	protected void reduce(Page pageName, Iterable<Page> pages, 
			Reducer<Page, Page, Text, NullWritable>.Context context)
			throws IOException, InterruptedException {

		for(Page page : pages) {
			context.write(new Text(page.toString()), NullWritable.get());
			break;
		}
	}
}
