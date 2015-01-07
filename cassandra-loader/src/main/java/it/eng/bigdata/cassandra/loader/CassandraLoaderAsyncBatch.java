package it.eng.bigdata.cassandra.loader;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BatchStatement.Type;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;

public class CassandraLoaderAsyncBatch implements CassandraLoader {
	
	private boolean usingGetUninterruptibly;
	
	public CassandraLoaderAsyncBatch() {
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
			Iterator<String> supplyIt = pointSource.iterator();
			while(supplyIt.hasNext()) {
				
				while(maxConcurrentWrites>0 && futures.size()>=maxConcurrentWrites) {
					ResultSetFuture future = futures.get(0);
					System.out.println(Thread.currentThread().getName() + " - " + "waiting for batch statement completion. Queue: " + futures.size() + "/" + maxConcurrentWrites);
					if(usingGetUninterruptibly) {
						future.getUninterruptibly();
					} else {
						future.get();
					}
					futures.remove(0);
				}
				
				BatchStatement batch = new BatchStatement(Type.UNLOGGED);
				batch.setConsistencyLevel(consistency);
				
				
				if(maxStatementsInBatch<=0) {
					maxStatementsInBatch = Integer.MAX_VALUE;
				}
				while(supplyIt.hasNext() && batch.size()<maxStatementsInBatch) {
					String supplyPoint = supplyIt.next();
					BoundStatement bound = pstm.bind(supplyPoint, day.substring(0, 6), clusteringDay, "A1", day, load, quality, 60, 24L*load.get(0), null, "OK");
					bound.setConsistencyLevel(consistency);
				
					batch.add(bound);
				}
				
				System.out.println("prepared batch of size: " + batch.size());
				
				futures.add(session.executeAsync(batch));
				System.out.println(Thread.currentThread().getName() + " - " + ++prg + " batch statements executed");
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
