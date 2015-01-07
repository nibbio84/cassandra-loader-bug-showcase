package it.eng.bigdata.cassandra.loader;

import java.util.LinkedList;
import java.util.List;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;

public class CassandraLoaderAsync implements CassandraLoader {

	private boolean usingGetUninterruptibly;
	
	public CassandraLoaderAsync() {
		this.usingGetUninterruptibly = Boolean.parseBoolean(System.getProperty("it.eng.get.unint", "true"));
	}
	
	@Override
	public void load(Cluster cluster, ConsistencyLevel consistency, String table, Iterable<String> pointSource,
			String day, Integer clusteringDay, List<Integer> load, List<Boolean> quality, int maxConcurrentWrites,  int maxStatementsInBatch) throws Exception {
		
		try (Session session = cluster.connect()) {
		
			List<ResultSetFuture> futures = new LinkedList<ResultSetFuture>();
			
			String cql = "insert into eng." + table + "(point_id, month, clustering_day, load_type, date, load_value, quality_word_value, sampling_interval, total_consumption, validation_codes, validation_status) values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
			PreparedStatement pstm = session.prepare(cql);
			
			
			long prg = 0;
			for(String supplyPoint : pointSource) {
				
				while(maxConcurrentWrites>0 && futures.size()>=maxConcurrentWrites) {
					ResultSetFuture future = futures.get(0);
					System.out.println(Thread.currentThread().getName() + " - " + "waiting for statement completion. Queue: " + futures.size() + "/" + maxConcurrentWrites);
					if(usingGetUninterruptibly) {
						future.getUninterruptibly();
					} else {
						future.get();
					}
					futures.remove(0);
				}
				
				BoundStatement bound = pstm.bind(supplyPoint, day.substring(0, 6), clusteringDay, "A1", day, load, quality, 60, 24L*load.get(0), null, "OK");
				bound.setConsistencyLevel(consistency);
				
				futures.add(session.executeAsync(bound));
				System.out.println(Thread.currentThread().getName() + " - " + ++prg + " statements executed");
			}
			
			while(futures.size()>0) {
				ResultSetFuture future = futures.get(0);
				System.out.println(Thread.currentThread().getName() + " - " + "waiting for statement completion. Queue: " + futures.size() + "/" + maxConcurrentWrites);
				if(usingGetUninterruptibly) {
					future.getUninterruptibly();
				} else {
					future.get();
				}
				futures.remove(0);
			}
		}
	}
	
}
