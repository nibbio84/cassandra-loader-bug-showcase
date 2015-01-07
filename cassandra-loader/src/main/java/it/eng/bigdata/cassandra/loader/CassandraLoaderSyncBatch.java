package it.eng.bigdata.cassandra.loader;

import java.util.Iterator;
import java.util.List;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BatchStatement.Type;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;

public class CassandraLoaderSyncBatch implements CassandraLoader {

	@Override
	public void load(Cluster cluster, ConsistencyLevel consistency, String table, Iterable<String> pointSource,
			String day, Integer clusteringDay, List<Integer> load, List<Boolean> quality, int maxConcurrentWrites,  int maxStatementsInBatch) throws Exception {
		
		try (Session session = cluster.connect()) {
		
			String cql = "insert into eng." + table + "(point_id, month, clustering_day, load_type, date, load_value, quality_word_value, sampling_interval, total_consumption, validation_codes, validation_status) values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
			PreparedStatement pstm = session.prepare(cql);
			
			long prg = 0;
			Iterator<String> supplyIt = pointSource.iterator();
			while(supplyIt.hasNext()) {
				
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
				
				session.execute(batch);
				System.out.println(Thread.currentThread().getName() + " - " + ++prg + " batch statements executed");
			}
		}
	}
	
}
