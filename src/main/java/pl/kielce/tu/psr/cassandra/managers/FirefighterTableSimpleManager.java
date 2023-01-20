package pl.kielce.tu.psr.cassandra.managers;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.data.UdtValue;

import java.util.Random;

public class FirefighterTableSimpleManager extends SimpleManager{

	public FirefighterTableSimpleManager(CqlSession session) {
		super(session);
	}

	public void createTable() {
		executeSimpleStatement(
				"CREATE TABLE firefighter (\n" +
				"    id int PRIMARY KEY,\n" +  
				"    name text,\n" +
				"    age int,\n" +
				");");
	}
	
	public void insertIntoTable() {
		executeSimpleStatement("INSERT INTO firefighter (id, name, age) " +
				" VALUES (1, 'Strażak Sam', 33);");
		executeSimpleStatement("INSERT INTO firefighter (id, name, age) " +
				" VALUES (2, 'Strażak Gonzalez', 44);");

	}

	public void addFirefighter(String name, int age) {
		Integer id = new Random().nextInt();
		executeSimpleStatement("INSERT INTO firefighter (id, name, age) " +
				String.format(" VALUES (%s, '%s', %s);",id, name, age));

	}

	public void updateIntoTable() {
		executeSimpleStatement("UPDATE firefighter SET age = 21 WHERE id = 1;");
	}

	public void deleteFromTable() {
		executeSimpleStatement("DELETE FROM firefighter;");
	}

	public void deleteFromTable(Long id) {
		executeSimpleStatement(String.format("DELETE FROM firefighter WHERE id = %s;", id));
	}

	public void selectFromTable() {
		String statement = "SELECT * FROM firefighter;";
		ResultSet resultSet = session.execute(statement);
		for (Row row : resultSet) {
			System.out.print("\tStrażak: ");
			System.out.print(row.getInt("id") + ", ");
			System.out.print(row.getString("name") + ", ");
			System.out.print(row.getInt("age") + ", ");
			System.out.println();
		}
		System.out.println("Statement \"" + statement + "\" executed successfully");
	}
	
	public void dropTable() {
		executeSimpleStatement("DROP TABLE firefighter;");
	}
}
