package it.eng.bigdata.cassandra.loader;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;

public class Load {

	public static void main(String[] args) throws Exception {
		
		if(args.length!=11) {
			System.out.println("This script launches a load towards a Cassandra cluster. It requires exactly 11 parameters in the following exact order.");
			System.out.println(
					"- cassandra_host (the contact host of the cassandra cluster)"
					+ "\n- consistency_level (ONE, QUORUM, ALL ...)"
					+ "\n- loader_type (the type of loader in use. Possible values: [Sync|Async|AsyncBatch|SyncBatch])"
					+ "\n- threads (the number 'n' of threads to use. Each thread gets about 'point_count/n' rows to write)"
					+ "\n- max_concurrent_writes_per_thread (the number of concurrent writes that each thread will execute in Async|AsyncBatch mode)"
					+ "\n- max_statements_in_batch (the maximum number of write operation present in each batch, in SyncBatch|AsyncBatch mode)"
					+ "\n- output_table (the output cassandra table. The keyspace is always 'eng') "
					+ "\n- point_start (the numeric identifier of the first point to use, each one identifies a partition key of type LOAD00..00XXX having 24 total characters)"
					+ "\n- point_count (the number of points to write)"
					+ "\n- day (the reference day in yyyyMMdd format)"
					+ "\n- clustering_day (an integer field used to group rows, you can use always 0)");
			return;
		}
		
		final String host = args[0];
		final ConsistencyLevel consistency = ConsistencyLevel.valueOf(args[1]);
		if(!"Sync".equals(args[2]) && !"Async".equals(args[2]) && !"AsyncBatch".equals(args[2]) && !"SyncBatch".equals(args[2])) {
			throw new IllegalArgumentException("Wrong loader type: " + args[2]);
		}
		@SuppressWarnings("unchecked")
		final Class<? extends CassandraLoader> loaderClass = (Class<? extends CassandraLoader>) Class.forName("it.eng.bigdata.cassandra.loader.CassandraLoader" + args[2]);
		final int threadNum = Integer.parseInt(args[3]);
		final int maxWritesPerThread = Integer.parseInt(args[4]);
		final int maxStatementsInBatch = Integer.parseInt(args[5]);
		final String table = args[6];
		final long pointStart = Long.parseLong(args[7]);
		final long pointCount = Long.parseLong(args[8]);
		final String day = args[9];
		final int clusteringDay = Integer.parseInt(args[10]);
		
		
		int fixedValue = 30;
		// Load is a fixed list of 24 integers
		final List<Integer> load = new ArrayList<>();
		for(int i=0; i<24; i++) {
			load.add(fixedValue);
		}
		
		final List<Boolean> quality = new ArrayList<>();
		for(int i=0; i<24; i++) {
			quality.add(true);
		}
		
		Thread[] threads;
			
		final Map<Integer, List<String>> splits = new HashMap<Integer, List<String>>();
		for(int t=0; t<threadNum; t++) {
			splits.put(t, new LinkedList<String>());
		}
		
		int bucket = 0;
		for(long i=pointStart; i<pointStart + pointCount; i++) {
			String supply = "LOAD" + pad(i, 20);
			
			splits.get(bucket).add(supply);
			bucket = (bucket + 1) % threadNum; 
		}
		
		threads = new Thread[threadNum];
		final AtomicInteger errors = new AtomicInteger(0);
		for(int t = 0; t<threadNum; t++) {
			final int threadCode = t;
			threads[t] = new Thread() {
				public void run() {
					try (Cluster cluster = Cluster.builder().addContactPoint(host).build()){
						
						CassandraLoader loader = loaderClass.newInstance();
						loader.load(cluster, consistency, table, splits.get(threadCode), day, clusteringDay, load, quality, maxWritesPerThread, maxStatementsInBatch);
					} catch(Exception e) {
						System.err.println("Thread " + threadCode + " morto");
						e.printStackTrace();
						errors.incrementAndGet();
					}
				}
			};
			threads[t].start();
		}
		
		System.out.println("Waiting threads completion");
		int prg = 1;
		for(Thread t : threads) {
			t.join();
			Thread.yield(); // prevent mixed prints
			System.out.println("Thread " + (prg++) + "/" + threads.length + " terminated");
		}
		
		Thread.yield();
		System.out.println("Threads terminated with error: " + errors.get());
		
		System.out.println("END");
		System.exit(0); // Not necessary
	}
	
	private static String pad(long num, int length) {
		StringBuilder bui = new StringBuilder();
		bui.append("" + num);
		while(bui.length()<length) {
			bui.insert(0, "0");
		}
		return bui.toString();
	}
	
}
