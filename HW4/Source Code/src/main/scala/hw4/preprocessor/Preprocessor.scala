package hw4.preprocessor

import org.apache.spark.rdd.RDD

import hw4.parser.Bz2WikiParser
import org.apache.spark.SparkContext
import java.io.Serializable

/*Preprocessor class receives the spark context as input, reads the input file,
 * takes each line and transforms it to an RDD with tuples of the form (String,
 * List[String]) where pageName is the key and its outlinks list is the value
 */
class Preprocessor(sc : SparkContext) extends Serializable {
  
	/**
	 * @param inputPath : String
	 * @return pairRDD : RDD[(String, List[String])]
	 * parseInputFile parses each line of the input file and applies a parser to
	 * it that takes the XML content and returns a string which is in the format
	 * that allows for easy transformation into an RDD of a required format
	 */
	def parseInputFile(inputPath : String) : RDD[(String, List[String])] = {
	  
	  	val fileContent = sc.textFile(inputPath);
	  	
	  	/*
	  	 * Takes each line in the input file and applies a parse function to it 
	  	 * that returns a string with the page name and its outlinks in a format
	  	 * that can be easily transformed in to an RDD of pageName and its list of 
	  	 * outlinks. The filter transformation filters out any erreneous lines 
	  	 * that might have thrown an exception while being parsed. The flatmap
	  	 * transformation splits the line delimited by "#", to take into account
	  	 * all the page's outlinks for page rank evaluation */
			val preprocessedRDD = fileContent
					.map(line => Bz2WikiParser.parseXML(line))
					.filter(preprocessedLine => (preprocessedLine.trim.length >= 1))
					.flatMap(line => line.split("#"));
			
			/*
			 * Takes the RDD of String and transforms it into RDD[(String,
			 * List[String])] that has been reduced so that there exists distinct page
			 * names as keys along with a list of its outlinks as values
			 */
			val pairRDD = preprocessedRDD
			.map(createPairRDD)
			.reduceByKey(_++_);
			
			return pairRDD;
	}
	

/**
 * @return (tring, List[String])
 * This helper function splits the incoming line at known points to create a
 * pairRDD with the pageName as key and its list of outlinks as values.
 */
def createPairRDD = {
    (line : String) =>
	  val lineSplit = line.split("~");
	  val pageName = lineSplit(0);
	  if(lineSplit.length > 1) {
	    val outlinks : List[String] = lineSplit(1).split("->").toList;
	    (pageName, outlinks);
	  }
	  else {
	    (pageName, List[String]());
	  }
	}
}