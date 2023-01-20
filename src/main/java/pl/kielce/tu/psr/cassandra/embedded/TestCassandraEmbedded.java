package pl.kielce.tu.psr.cassandra.embedded;

import java.net.InetSocketAddress;
import java.util.Scanner;

import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.Level;

import com.datastax.oss.driver.api.core.CqlSession;
import com.github.nosan.embedded.cassandra.Cassandra;
import com.github.nosan.embedded.cassandra.CassandraBuilder;
import com.github.nosan.embedded.cassandra.Settings;

import pl.kielce.tu.psr.cassandra.managers.KeyspaceSimpleManager;
import pl.kielce.tu.psr.cassandra.managers.FirefighterTableSimpleManager;

public class TestCassandraEmbedded {

	public static void main(String[] args) {
		Configurator.setLevel("com.github.nosan.embedded.cassandra.Cassandra", Level.FATAL);
		
		Cassandra cassandra = new CassandraBuilder().build();
		cassandra.start();
		try {
			Settings settings = cassandra.getSettings();
			try (CqlSession session = CqlSession.builder()
					.addContactPoint(new InetSocketAddress(settings.getAddress(), settings.getPort()))
					.withLocalDatacenter("datacenter1").build()) {
				
				KeyspaceSimpleManager keyspaceManager = new KeyspaceSimpleManager(session, "university");
				keyspaceManager.dropKeyspace();
				keyspaceManager.selectKeyspaces();
				keyspaceManager.createKeyspace();
				keyspaceManager.useKeyspace();

				FirefighterTableSimpleManager tableManager = new FirefighterTableSimpleManager(session);
				tableManager.createTable();
				tableManager.insertIntoTable();
				tableManager.selectFromTable();

				while (true)  {
					System.out.println("Wprowadź nr operacji: \n 1 - wyswietl liste strażaków \n 2 - dodaj strażaka \n 3 - skasuj strażaka\n 0 - wyjście");
					Scanner s= new Scanner(System.in);
					char operationNumber = s.next().charAt(0);
					if (Character.getNumericValue(operationNumber) == 0) {
						break;
					}
					processUserInput(operationNumber, tableManager);
				}

				tableManager.deleteFromTable();
				tableManager.dropTable();
				
			}
		} finally {
			cassandra.stop();
		}

	}

	private static void processUserInput(int userInput, FirefighterTableSimpleManager tableManager) {
		int numericValue = Character.getNumericValue(userInput);
		switch(numericValue) {
			case 1:
				getAllFirefighters(tableManager);
				break;
			case 2:
				addFirefighter(tableManager);
				break;
			case 3:
				deleteFirefighter(tableManager);
				break;
			default:
				System.out.println("Wybor niewlasciwy, sprobuj raz jeszcze.\n");
		}
	}

	private static void deleteFirefighter(FirefighterTableSimpleManager tableManager) {
		System.out.println("Którego strażaka usunąć? Podaj jego id:\n");
		Scanner s= new Scanner(System.in);
		String key = s.nextLine();
		long id = Long.parseLong(key);
		tableManager.deleteFromTable(id);
	}

	private static void addFirefighter(FirefighterTableSimpleManager tableManager) {
		System.out.println("Podaj nazwisko strażaka:\n");
		Scanner s = new Scanner(System.in);
		String name = s.nextLine();
		System.out.println("Podaj wiek strażaka:\n");
		String age = s.nextLine();
		tableManager.addFirefighter(name, Integer.parseInt(age));
	}

	private static void getAllFirefighters(FirefighterTableSimpleManager tableManager) {
		tableManager.selectFromTable();
	}

}