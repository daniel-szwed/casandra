package pl.kielce.tu.psr.cassandra.simple;

import com.datastax.oss.driver.api.core.CqlSession;

import pl.kielce.tu.psr.cassandra.managers.KeyspaceSimpleManager;
import pl.kielce.tu.psr.cassandra.managers.FirefighterTableSimpleManager;

public class TestCassandraSimple {
	public static void main(String[] args) {
		try (CqlSession session = CqlSession.builder().build()) {
			KeyspaceSimpleManager keyspaceManager = new KeyspaceSimpleManager(session, "university");
			keyspaceManager.dropKeyspace();
			keyspaceManager.selectKeyspaces();
			keyspaceManager.createKeyspace();
			keyspaceManager.useKeyspace();
			
			FirefighterTableSimpleManager tableManager = new FirefighterTableSimpleManager(session);
			tableManager.createTable();
			tableManager.insertIntoTable();
			tableManager.updateIntoTable();
			tableManager.selectFromTable();
			tableManager.deleteFromTable();
			tableManager.dropTable();
		}
	}
}