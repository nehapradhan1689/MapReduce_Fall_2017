package hw4.pagerank

import org.apache.spark.rdd.RDD
import java.io.Serializable

/*
 * PageRankSpark class receives the noOfPages as input, takes each tuple from
 * the input RDD with page rank and calculates the page rank for the current 
 * iteration for each page
 */
class PageRankSpark(noOfPages : Double) extends Serializable{
  
  val totalNoOfPages : Double = noOfPages;
  val alpha : Double = 0.15;
  val alphaInverse : Double = 0.85;
  val alphaContribution = alpha / totalNoOfPages;  

	/**
	 * @param pageRankRDD : RDD[(String, (List[String], Double))]
	 * @param danglingLinkMass : Double
	 * @return newPageRankRDD : RDD[(String, (List[String], Double))]
	 * computePageRank takes an RDD with pageName, list of outlinks and its page
	 * rank and computes the page rank of the page for the current iteration
	 */
	def computePageRank(pageRankRDD :  RDD[(String, (List[String], Double))], 
			danglingLinkMass : Double) : RDD[(String, (List[String], Double))] = {

	        /*
	         * pageRankRDD is transformed to an RDD that only has each outlink's pageName
	         * as key and the factor of pageRank contribution from the current page as
	         * its value. The current page is also added to this RDD with the pageRank
	         * set to zero, so that it is accounted for in case it is a page with no 
	         * in-links
	         */
					val outlinksPageRanksDistribution = pageRankRDD
							.flatMap{ case(pageName, (urls, rank)) => 
							val currentRecord = (pageName, 0.0);
							val outLinksSize = urls.size;
							urls.map(url => (url, rank/outLinksSize)).union(List(currentRecord));
					}
					
					/*
					 * The RDD created with the pageName and pageRank contribution towards it
					 * is reduced by key which sums up all the pageRank contributions for a
					 * particular page and then goes on to evaluate its final pageRank for the
					 * current iteration using the pageRank formula and taking into account
					 * dangling node contribution
					 */
					val reducedPageRanksRDD = outlinksPageRanksDistribution
							.reduceByKey((sum, pr) => sum + pr)
							.mapValues(v => (alphaContribution + (alphaInverse * (danglingLinkMass + v))));

					/*
					 * This transformation is done merely to bring the RDD to be returned in the 
					 * required format so that it can be processed until the pageRank values 
					 * converge
					 */
					val newPageRankRDD = pageRankRDD.join(reducedPageRanksRDD)
							.map{ case(pageName, ((urls, oldPR), newPR)) => (pageName, (urls, newPR))};
					
					return newPageRankRDD;
	}
}