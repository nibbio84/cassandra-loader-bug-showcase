package it.eng.bigdata.cassandra.loader;

import java.util.List;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;

public interface CassandraLoader {

	public void load(Cluster cluster, ConsistencyLevel consistency, String table, Iterable<String> pointSource, String day, Integer clusteringDay, List<Integer> loadValue, List<Boolean> quality, int maxConcurrentWrites,  int maxStatementsInBatch) throws Exception;
	
}
