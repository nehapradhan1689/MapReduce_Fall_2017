package assignment1;

import java.util.List;

public class WorkPartitioner {
	private List<String> linesFromFile;
	
	public WorkPartitioner(List<String> linesFromFile) {
		super();
		this.linesFromFile = linesFromFile;
	}

	//This splits the input file for processing by two threads with no lock on data structure
	public void partitionInputForThreads() {
		Thread[] workerThreads = new Thread[ConstantClass.MAX_THREADS];
		int chunkSize = linesFromFile.size()/ConstantClass.MAX_THREADS;
		for(int i = 0; i < ConstantClass.MAX_THREADS; i++) {
			int startIndex = i * chunkSize;
			int endIndex = 0;
			if(i >= ConstantClass.MAX_THREADS - 1) {
				endIndex = linesFromFile.size() - 1;
			}
			else {
				endIndex = startIndex + chunkSize;
			}
			List<String> chunkContent = linesFromFile.subList(startIndex, endIndex);
			workerThreads[i] = new Thread(new AverageTMAXNoLock(chunkContent));
			workerThreads[i].start();
		}
		for(int j = 0; j < workerThreads.length; j++) {
			try {
				workerThreads[j].join();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	//This splits the input file for processing by two threads with coarse lock on data structure
	public void partitionInputForThreadsCoarseLock() {
		Thread[] workerThreads = new Thread[ConstantClass.MAX_THREADS];
		int chunkSize = linesFromFile.size()/ConstantClass.MAX_THREADS;
		for(int i = 0; i < ConstantClass.MAX_THREADS; i++) {
			int startIndex = i * chunkSize;
			int endIndex = 0;
			if(i >= ConstantClass.MAX_THREADS - 1) {
				endIndex = linesFromFile.size() - 1;
			}
			else {
				endIndex = startIndex + chunkSize;
			}
			List<String> chunkContent = linesFromFile.subList(startIndex, endIndex);
			workerThreads[i] = new Thread(new AverageTMAXCoarseLock(chunkContent));
			workerThreads[i].start();
		}
		for(int j = 0; j < workerThreads.length; j++) {
			try {
				workerThreads[j].join();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	//This splits the input file for processing by two threads with fine lock on data structure
	public void partitionInputForThreadsFineLock() {
		Thread[] workerThreads = new Thread[ConstantClass.MAX_THREADS];
		int chunkSize = linesFromFile.size()/ConstantClass.MAX_THREADS;
		for(int i = 0; i < ConstantClass.MAX_THREADS; i++) {
			int startIndex = i * chunkSize;
			int endIndex = 0;
			if(i >= ConstantClass.MAX_THREADS - 1) {
				endIndex = linesFromFile.size() - 1;
			}
			else {
				endIndex = startIndex + chunkSize;
			}
			List<String> chunkContent = linesFromFile.subList(startIndex, endIndex);
			workerThreads[i] = new Thread(new AverageTMAXFineLock(chunkContent));
			workerThreads[i].start();
		}
		for(int j = 0; j < workerThreads.length; j++) {
			try {
				workerThreads[j].join();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	//This splits the input file for processing by two threads with no sharing of data structure
	public void partitionInputForThreadsNoSharing() {
		Thread[] workerThreads = new Thread[ConstantClass.MAX_THREADS];
		AverageTMAXNoSharing[] resultantObject = new AverageTMAXNoSharing[ConstantClass.MAX_THREADS];
		int chunkSize = linesFromFile.size()/ConstantClass.MAX_THREADS;
		for(int i = 0; i < ConstantClass.MAX_THREADS; i++) {
			int startIndex = i * chunkSize;
			int endIndex = 0;
			if(i >= ConstantClass.MAX_THREADS - 1) {
				endIndex = linesFromFile.size() - 1;
			}
			else {
				endIndex = startIndex + chunkSize;
			}
			List<String> chunkContent = linesFromFile.subList(startIndex, endIndex);
			resultantObject[i] = new AverageTMAXNoSharing(chunkContent);
			workerThreads[i] = new Thread(resultantObject[i]);
			workerThreads[i].start();
		}
		for(int j = 0; j < workerThreads.length; j++) {
			try {
				workerThreads[j].join();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		CombinerClass combineResults = new CombinerClass(resultantObject);
		combineResults.combiner();
	}

}
