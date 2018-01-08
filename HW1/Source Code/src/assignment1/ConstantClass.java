package assignment1;

import java.util.HashMap;
import java.util.Map;

public class ConstantClass {

	public static int REC_STATION_ID = 0;
	public static int REC_OBSV_TYPE_ID = 2;
	public static int REC_OBSV_VALUE_ID = 3;
	public static Map<String, StationStats> SEQACCUMULATOR = new HashMap<String, StationStats>();
	public static Map<String, StationStats> ACCUMULATOR = new HashMap<String, StationStats>();
	public static Map<String, StationStats> COARSE_LOCK_ACCUMULATOR = new HashMap<String, StationStats>();
	public static Map<String, StationStats> FINE_LOCK_ACCUMULATOR = new HashMap<String, StationStats>();
	public static Map<String,StationStats> NO_SHARING_ACCUMULATOR = new HashMap<String, StationStats>();
	public static int MAX_THREADS = 2;
	
}
