package assignment1;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.zip.GZIPInputStream;

public class InputLoader {
	private static long totalTime = 0;
	private static long minTime = Long.MAX_VALUE;
	private static long maxTime = Long.MIN_VALUE;
	private static double averageTime = 0;
	private static List<String> fileContent = new ArrayList<String>();
	public static String MAKE_EXPENSIVE_RUN;

	//This function reads the input file from the path entered by the user and stores it as a String list
	private static void loader() {
		System.out.println("Enter filename: \n");
		Scanner scan = new Scanner(System.in);
		String filename = scan.nextLine();
		System.out.println("Do you want to make the run expensive? Y/N");
		MAKE_EXPENSIVE_RUN = scan.nextLine();
		File newFile = new File(filename);

		String line = null;

		try {

			InputStream fileStream = new FileInputStream(newFile);
			InputStream gzipStream = new GZIPInputStream(fileStream);
			InputStreamReader reader = new InputStreamReader(gzipStream);
			BufferedReader buffered = new BufferedReader(reader);

			while ((line = buffered.readLine()) != null) {
				fileContent.add(line);
			}

			buffered.close();  
			scan.close();
		}
		catch(FileNotFoundException ex) {
			System.out.println("Unable to open file '" + filename + "'");                
		}
		catch(IOException ex) {
			System.out.println("Error reading file '" + filename + "'");                  
		}		
	}

	public static void main(String args[]) {

		//Loading the input file from the given location
		loader();

		executeSequential();

		//Creating an instance of type WorkPartitioner to split the work and assign it to different threads
		WorkPartitioner wp = new WorkPartitioner(fileContent);

		executeNoLock(wp);

		executeCoarseLock(wp);	

		executeFineLock(wp);

		executeNoSharing(wp);
	}

	//Computing the average temperature of stations using multi-threading with no sharing of datastructures 
	//and finding the time taken for execution over 10 runs	
	private static void executeNoSharing(WorkPartitioner wp) {	
		System.out.println("Computing time for No Sharing:");
		for(int i = 0; i < 10; i++) {
			ConstantClass.NO_SHARING_ACCUMULATOR = new HashMap<String, StationStats>();
			Long timeBeforeCall = System.currentTimeMillis();
			wp.partitionInputForThreadsNoSharing();
			Long timeAfterCall = System.currentTimeMillis();
			Long timeTaken = timeAfterCall - timeBeforeCall;
			totalTime += timeTaken;
			minTime = Long.min(timeTaken,minTime);
			maxTime = Long.max(timeTaken, maxTime);
			System.out.println("Time taken for run " + (i+1) +": " +timeTaken);

		}
		averageTime = totalTime/10;
		System.out.println("Minimum Time Taken: " + minTime + "\t Maximum Time Taken: " + maxTime + "\t Average time:" + averageTime);

		System.out.println("**********Computing time for No Sharing Ended**********");

		resetTimeVariables();
		
		try {
			FileWriter fw = new FileWriter("NoSharingOutput.txt",true);
			BufferedWriter br = new BufferedWriter(fw);
			for(Map.Entry<String,StationStats> entry: ConstantClass.NO_SHARING_ACCUMULATOR.entrySet()){
				String str = entry.getKey()+" : "+entry.getValue() + "\n";
				br.write(str);
			}
			br.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	//Computing the average temperature of stations using multi-threading with fine locks 
	//and finding the time taken for execution over 10 runs
	private static void executeFineLock(WorkPartitioner wp) {		
		System.out.println("Computing time for Fine Lock:");
		for(int i = 0; i < 10; i++) {
			ConstantClass.FINE_LOCK_ACCUMULATOR = new HashMap<String, StationStats>();
			Long timeBeforeCall = System.currentTimeMillis();
			wp.partitionInputForThreadsFineLock();
			Long timeAfterCall = System.currentTimeMillis();
			Long timeTaken = timeAfterCall - timeBeforeCall;
			totalTime += timeTaken;
			minTime = Long.min(timeTaken,minTime);
			maxTime = Long.max(timeTaken, maxTime);
			System.out.println("Time taken for run " + (i+1) +": " +timeTaken);

		}
		averageTime = totalTime/10;
		System.out.println("Minimum Time Taken: " + minTime + "\t Maximum Time Taken: " + maxTime + "\t Average time:" + averageTime);

		System.out.println("**********Computing time for Fine Lock Ended**********");

		resetTimeVariables();
		
		try {
			FileWriter fw = new FileWriter("FineLockOutput.txt",true);
			BufferedWriter br = new BufferedWriter(fw);
			for(Map.Entry<String,StationStats> entry: ConstantClass.FINE_LOCK_ACCUMULATOR.entrySet()){
				String str = entry.getKey()+" : "+entry.getValue() + "\n";
				br.write(str);
			}
			br.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

	//Computing the average temperature of stations using multi-threading with coarse locks 
	//and finding the time taken for execution over 10 runs
	private static void executeCoarseLock(WorkPartitioner wp) {
		System.out.println("Computing time for Coarse Lock:");
		for(int i = 0; i < 10; i++) {
			ConstantClass.COARSE_LOCK_ACCUMULATOR = new HashMap<String, StationStats>();
			Long timeBeforeCall = System.currentTimeMillis();
			wp.partitionInputForThreadsCoarseLock();
			Long timeAfterCall = System.currentTimeMillis();
			Long timeTaken = timeAfterCall - timeBeforeCall;
			totalTime += timeTaken;
			minTime = Long.min(timeTaken,minTime);
			maxTime = Long.max(timeTaken, maxTime);
			System.out.println("Time taken for run " + (i+1) +": " +timeTaken);

		}
		averageTime = totalTime/10;
		System.out.println("Minimum Time Taken: " + minTime + "\t Maximum Time Taken: " + maxTime + "\t Average time:" + averageTime);

		System.out.println("**********Computing time for Coarse Lock Ended**********");

		resetTimeVariables();
		
		try {
			FileWriter fw = new FileWriter("CoarseLockOutput.txt",true);
			BufferedWriter br = new BufferedWriter(fw);
			for(Map.Entry<String,StationStats> entry: ConstantClass.COARSE_LOCK_ACCUMULATOR.entrySet()){
				String str = entry.getKey()+" : "+entry.getValue() + "\n";
				br.write(str);
			}
			br.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

	//Computing the average temperature of stations using multi-threading with no locks 
	//and finding the time taken for execution over 10 runs
	private static void executeNoLock(WorkPartitioner wp) {
		System.out.println("Computing time for No Lock:");
		for(int i = 0; i < 10; i++) {
			ConstantClass.ACCUMULATOR = new HashMap<String, StationStats>();
			Long timeBeforeCall = System.currentTimeMillis();
			wp.partitionInputForThreads();
			Long timeAfterCall = System.currentTimeMillis();
			Long timeTaken = timeAfterCall - timeBeforeCall;
			totalTime += timeTaken;
			minTime = Long.min(timeTaken,minTime);
			maxTime = Long.max(timeTaken, maxTime);
			System.out.println("Time taken for run " + (i+1) +": " +timeTaken);

		}
		averageTime = totalTime/10;
		System.out.println("Minimum Time Taken: " + minTime + "\t Maximum Time Taken: " + maxTime + "\t Average time:" + averageTime);

		System.out.println("**********Computing time for No Lock Ended**********");

		resetTimeVariables();
		
		try {
			FileWriter fw = new FileWriter("NoLockOutput.txt",true);
			BufferedWriter br = new BufferedWriter(fw);
			for(Map.Entry<String,StationStats> entry: ConstantClass.ACCUMULATOR.entrySet()){
				String str = entry.getKey()+" : "+entry.getValue() + "\n";
				br.write(str);
			}
			br.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	//Computing the average temperature of stations sequentially and finding the time taken for execution over 10 runs
	private static void executeSequential() {
		AverageTMAXSeq stnTempSeq = new AverageTMAXSeq(fileContent);
		System.out.println("Computing time for Sequential Computation:");
		for(int i = 0; i < 10; i++) {
			ConstantClass.SEQACCUMULATOR = new HashMap<String, StationStats>();
			Long timeBeforeCall = System.currentTimeMillis();
			stnTempSeq.findAvgTMAXSeq();
			Long timeAfterCall = System.currentTimeMillis();
			Long timeTaken = timeAfterCall - timeBeforeCall;
			totalTime += timeTaken;
			minTime = Long.min(timeTaken,minTime);
			maxTime = Long.max(timeTaken, maxTime);
			System.out.println("Time taken for run " + (i+1) +": " +timeTaken);
			
		}
		averageTime = totalTime/10;
		System.out.println("Minimum Time Taken: " + minTime + "\t Maximum Time Taken: " + maxTime + "\t Average time:" + averageTime);

		System.out.println("**********Computing time for Sequential Computation Ended**********");

		resetTimeVariables();
		
		try {
			FileWriter fw = new FileWriter("SequentialOutput.txt",true);
			BufferedWriter br = new BufferedWriter(fw);
			for(Map.Entry<String,StationStats> entry: ConstantClass.SEQACCUMULATOR.entrySet()){
				String str = entry.getKey()+" : "+entry.getValue() + "\n";
				br.write(str);
			}
			br.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	//This function resets the time variables for re-use for other function calls
	private static void resetTimeVariables() {
		// TODO Auto-generated method stub
		totalTime = 0;
		minTime = Long.MAX_VALUE;
		maxTime = Long.MIN_VALUE;
		averageTime = 0;
	}
}
