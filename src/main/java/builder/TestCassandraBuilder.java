package builder;

import com.datastax.oss.driver.api.core.CqlSession;

import java.util.Scanner;
import java.util.logging.Level;
import java.util.logging.Logger;

public class TestCassandraBuilder {
	public static void main(String[] args) {
		Logger logger = Logger.getLogger("");
		logger.setLevel(Level.OFF);
		CqlSession session = CqlSession.builder().build();
		KeyspaceBuilderManager keyspaceManager = new KeyspaceBuilderManager(session, "firmaPrzewozowa");
		keyspaceManager.dropKeyspace();
		keyspaceManager.selectKeyspaces();
		keyspaceManager.createKeyspace();
		keyspaceManager.useKeyspace();

		TableBuilderManager tableManager = new TableBuilderManager(session);
		tableManager.createTable();
		Scanner scanner = new Scanner(System.in);

		do {
			System.out.println("|=================> System zarządzania firmą przewozową <=================|");
			System.out.println("===> 1.Dodaj zlecenie");
			System.out.println("===> 2.Wyświetl wszystkie zlecenia");
			System.out.println("===> 3.Usuń zlecenie");
			System.out.println("===> 4.Edytuj zlecenie");
			System.out.println("===> 5.Szukaj zlecenia po id");
			System.out.println("===> 6.Wyszukiwanie zlecenia po typie oraz wadze");
			System.out.println("===> 7.Zliczanie ilości zleceń po statusie");

			System.out.println("Wybór:");
			int choice = scanner.nextInt();
			scanner.nextLine();
			switch (choice) {
				case 1:
					tableManager.insertIntoTable(scanner);
					break;
				case 2:
					tableManager.selectFromTable();
					break;
				case 3:
					tableManager.selectFromTable();
					tableManager.deleteFromTable(scanner);
					break;
				case 4:
					tableManager.selectFromTable();
					tableManager.updateTable(scanner);
					tableManager.selectFromTable();
					break;
				case 5:
					tableManager.selectFromTable();
					tableManager.selectById(scanner);
					break;
				case 6:
					tableManager.selectByTypeAndWeight(scanner);
					break;
				case 7:
					tableManager.selectCount(scanner);
					break;
			}
		} while(true);
	}
}