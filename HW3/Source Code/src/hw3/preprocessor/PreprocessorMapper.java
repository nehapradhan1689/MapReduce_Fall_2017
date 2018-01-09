package hw3.preprocessor;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;

import hw3.datastructures.Page;
import hw3.parser.WikiParser;

//The Mapper takes each line of the input file and extracts the links in its 
//corresponding XML representation based on certain criteria. In this program
//each incoming page with details of its outlinks is emitted. The outlinks 
//corresponding to a page are also emitted with a default value to keep track
//of dangling nodes which play a part in evaluting page rank of pages
public class PreprocessorMapper extends Mapper<LongWritable, Text, Page, Page> {

	private static Pattern namePattern;
	private static XMLReader xmlReader;
	int counter = 0;
	static {
		// Keep only html pages not containing tilde (~).
		namePattern = Pattern.compile("^([^~]+)$");
		// Configure parser.
		SAXParserFactory spf = SAXParserFactory.newInstance();
		try {
			spf.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
			SAXParser saxParser = spf.newSAXParser();
			xmlReader = saxParser.getXMLReader();
		} catch (ParserConfigurationException | SAXException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Page, Page>.Context context)
			throws IOException, InterruptedException {

		// Parser fills this list with linked page names.
		List<String> linkPageNames = new LinkedList<String>();
		xmlReader.setContentHandler(new WikiParser(linkPageNames));
		String line = value.toString();
		// Each line formatted as (Wiki-page-name:Wiki-page-html).
		int delimLoc = line.indexOf(':');
		String pageName = line.substring(0, delimLoc);
		String html = line.substring(delimLoc + 1);
		html = html.replace("&", "&amp;");
		Matcher matcher = namePattern.matcher(pageName);
		if (matcher.find()) {

			//Process file as name does not contain (~).
			// Parse page and fill list of linked pages.
			linkPageNames.clear();
			try {
				xmlReader.parse(new InputSource(new StringReader(html)));

				//Removing duplicate page names from outlinks list
				List<String> noDupOutlinks = new ArrayList<String>();
				for(String oLink : linkPageNames) {
					if(!noDupOutlinks.contains(oLink) && !oLink.equals(pageName)) {
						noDupOutlinks.add(oLink);
						Page outlinkPage = new Page(new Text(oLink));
						//Emitting each outlink to keep track of dangling nodes
						context.write(outlinkPage, outlinkPage);
					}
				}
				Page page = new Page(new Text(pageName), 
						new IntWritable(noDupOutlinks.size()), noDupOutlinks);
				//Emitting each page with their corresponding outlinks with user
				//defined datatype
				context.write(page, page);
			} catch (Exception e) {
				// Discard ill-formatted pages.
			}
		}
	}
}

