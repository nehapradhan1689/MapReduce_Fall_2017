package hw4.driver

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.HashPartitioner


import hw4.preprocessor.Preprocessor
import hw4.pagerank.PageRankSpark
import java.io.Serializable

object Driver extends Serializable {

	/**
	 * @param args
	 */
	def main(args: Array[String]): Unit = {

			/*Setting up configurations for local and AWS run*/
			//			val conf = new SparkConf().setAppName("Page Rank Spark").setMaster("local[*]");
			val conf = new SparkConf().setAppName("Page Rank Spark").setMaster("yarn");
			val sc = new SparkContext(conf);

			/*
			 * Preprocessing is acheived by calling a function in the Preprocessor class
			 * that takes the Spark Context as argument, reads the input file and
			 * transforms each line in the input to a preprocessed format. The resulting
			 * preprocessed RDD will be a collection of tuples of the form (String,
			 * List[String])) where pageName is the key and its outlinks are the values
			 */
			val preprocessObject = new Preprocessor(sc);
			val preprocessedPairRDD = preprocessObject.parseInputFile(args(0));

			val noOfPages : Double = preprocessedPairRDD.count();

			val startingPR : Double = 1.0 / noOfPages;

			/*Transforming the RDD to include a default page rank before the first run*/
			var pairRDDWithPR = preprocessedPairRDD
					.map{ case(k, v) => (k, (v, startingPR)) }
			.persist;

			/*
			 * PageRank computation is delegated to the PageRankSpark class which computes 
			 * the page rank of all the pages present in the RDD
			 */ 
			val pageRankObject = new PageRankSpark(noOfPages);

			/*
			 * Running the PageRank module ten times to allow page ranks to converge for 
			 * all pages
			 */ 
			for(i <- 1 to 10) {

				/*
				 * Computing the dangling node contribution to the page rank for this
				 * iteration.This is an action and it calcuates the total of the page
				 * ranks of dangling nodes to distribute among all the pages
				 */
				val danglingPageRanksSum = pairRDDWithPR.values.filter(record => (record._1.isEmpty)).values.sum();
				val danglingLinkMass = danglingPageRanksSum / noOfPages;

				/*
				 *  PageRank evaluation is acheived by calling a function in the PageRankSpark 
				 *  class that takes an RDD and the dangling node contribution and evaluates 
				 *  the resulting RDD will be a collection of tuples of the form (String,
				 *  (List[String], Double)) where pageName is the key and its outlinks and
				 *  page rank are the values
				 */
				pairRDDWithPR = pageRankObject.computePageRank(pairRDDWithPR, danglingLinkMass);

			}

			/*
			 * Takes the top 100 pages alongwith their page ranks and saves the results to 
			 * an output file
			 */
			val topKRDD = sc.parallelize(pairRDDWithPR.map(record => (record._2._2, record._1)).top(100), 1)
					.saveAsTextFile(args(1));
			sc.stop();
	}

}