package builder;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.api.querybuilder.delete.Delete;
import com.datastax.oss.driver.api.querybuilder.insert.Insert;
import com.datastax.oss.driver.api.querybuilder.relation.Relation;
import com.datastax.oss.driver.api.querybuilder.schema.CreateTable;
import com.datastax.oss.driver.api.querybuilder.schema.CreateType;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.datastax.oss.driver.api.querybuilder.update.Update;
import simple.SimpleManager;

import java.util.Scanner;
import java.util.UUID;

public class TableBuilderManager extends SimpleManager {

	public TableBuilderManager(CqlSession session) {
		super(session);
	}

	public void createTable() {
		CreateType createType = SchemaBuilder.createType("driver").withField("firstName", DataTypes.TEXT)
				.withField("lastName", DataTypes.TEXT).withField("drivingLicenseNumber", DataTypes.INT);

		session.execute(createType.build());

		CreateTable createTable = SchemaBuilder.createTable("transport")
				.withPartitionKey("id", DataTypes.TEXT)
				.withColumn("name", DataTypes.TEXT)
				.withColumn("type", DataTypes.TEXT)
				.withColumn("weight", DataTypes.DOUBLE)
				.withColumn("postingAddress", DataTypes.TEXT)
				.withColumn("postingDate", DataTypes.TEXT)
				.withColumn("shippingAddress", DataTypes.TEXT)
				.withColumn("shippingDate", DataTypes.TEXT)
				.withColumn("status", DataTypes.TEXT)
				.withColumn("driver", QueryBuilder.udt("driver"));
		session.execute(createTable.build());
	}

	public void insertIntoTable(Scanner scanner) {
		System.out.println("Nazwa przesyłki");
		String name = scanner.nextLine();
		System.out.println("Rodzaj przesyłki");
		String type = scanner.nextLine();
		System.out.println("Waga przesyłki (Wartość w tonach)");
		Double weight = scanner.nextDouble();
		scanner.nextLine();
		System.out.println("Adres nadania przesyłki");
		String postingAddress = scanner.nextLine();
		System.out.println("Data nadania przesyłki (DD-MM-RRRR)");
		String postingDate = scanner.nextLine();
		System.out.println("Adres dostarczenia przesyłki");
		String shippingAddress = scanner.nextLine();
		System.out.println("Data dostarczenia przesyłki (DD-MM-RRRR)");
		String shippingDate = scanner.nextLine();
		System.out.println("Status przesyłki");
		String status = scanner.nextLine();
		System.out.println("Imię kierowcy");
		String firstName = scanner.nextLine();
		System.out.println("Nazwisko kierowcy");
		String lastName = scanner.nextLine();
		System.out.println("Numer prawa jazdy kierowcy");
		Integer drivingLicenseNumber = scanner.nextInt();
		scanner.nextLine();
		Insert insert = QueryBuilder.insertInto("firmaPrzewozowa", "transport")
				.value("id", QueryBuilder.literal(String.valueOf(UUID.randomUUID())))
				.value("name", QueryBuilder.literal(name))
				.value("type", QueryBuilder.literal(type))
				.value("weight", QueryBuilder.literal(weight))
				.value("postingAddress", QueryBuilder.literal(postingAddress))
				.value("postingDate", QueryBuilder.literal(postingDate))
				.value("shippingAddress", QueryBuilder.literal(shippingAddress))
				.value("shippingDate", QueryBuilder.literal(shippingDate))
				.value("status", QueryBuilder.literal(status))
				.value("driver", QueryBuilder.raw("{firstName : '"+firstName+"', lastName : '"+lastName+"', drivingLicenseNumber : "+drivingLicenseNumber+"}"));
		session.execute(insert.build());
	}

	public void updateTable(Scanner scanner) {
		System.out.println("Podaj id które ma być edytowane");
		String id = scanner.nextLine();
		System.out.println("Wybierz pole które chcesz edytować");
		System.out.println(" 1.  Nazwa przesyłki");
		System.out.println(" 2.  Rodzaj przesyłki");
		System.out.println(" 3.  Waga przesyłki (Wartość w tonach)");
		System.out.println(" 4.  Adres nadania przesyłki");
		System.out.println(" 5.  Data nadania przesyłki (DD-MM-RRRR)");
		System.out.println(" 6.  Adres dostarczenia przesyłki");
		System.out.println(" 7.  Data dostarczenia przesyłki (DD-MM-RRRR)");
		System.out.println(" 8.  Status przesyłki");
		System.out.println(" 9.  Imię kierowcy");
		System.out.println("10.  Nazwisko kierowcy");
		System.out.println("11.  Numer prawa jazdy kierowcy");
		int choice = scanner.nextInt();
		scanner.nextLine();
		switch (choice) {
			case 1:
				System.out.println("Wpisz nową nazwę przesyłki");
				String newName = scanner.nextLine();
				Update updateName = QueryBuilder.update("transport").setColumn("name", QueryBuilder.literal(newName)).whereColumn("id").isEqualTo(QueryBuilder.literal(id));
				session.execute(updateName.build());
				break;
			case 2:
				System.out.println("Wpisz nowy rodzaj przesyłki");
				String newType = scanner.nextLine();
				Update updateType = QueryBuilder.update("transport").setColumn("type", QueryBuilder.literal(newType)).whereColumn("id").isEqualTo(QueryBuilder.literal(id));
				session.execute(updateType.build());
				break;
			case 3:
				System.out.println("Wpisz nową wagę przesyłki (Wartość w tonach)");
				String newWeight = scanner.nextLine();
				Update updateWeight = QueryBuilder.update("transport").setColumn("weight", QueryBuilder.literal(newWeight)).whereColumn("id").isEqualTo(QueryBuilder.literal(id));
				session.execute(updateWeight.build());
				break;
			case 4:
				System.out.println("Wpisz nowy adres nadania przesyłki");
				String newPostingAddress = scanner.nextLine();
				Update updatePostingAddress = QueryBuilder.update("transport").setColumn("postingAddress", QueryBuilder.literal(newPostingAddress)).whereColumn("id").isEqualTo(QueryBuilder.literal(id));
				session.execute(updatePostingAddress.build());
				break;
			case 5:
				System.out.println("Wpisz nową datę nadania przesyłki (DD-MM-RRRR)");
				String newPostingDate = scanner.nextLine();
				Update updatePostingDate = QueryBuilder.update("transport").setColumn("postingDate", QueryBuilder.literal(newPostingDate)).whereColumn("id").isEqualTo(QueryBuilder.literal(id));
				session.execute(updatePostingDate.build());
				break;
			case 6:
				System.out.println("Wpisz nowy adres dostarczenia przesyłki");
				String newShippingAddress = scanner.nextLine();
				Update updateShippingAddress = QueryBuilder.update("transport").setColumn("shippingAddress", QueryBuilder.literal(newShippingAddress)).whereColumn("id").isEqualTo(QueryBuilder.literal(id));
				session.execute(updateShippingAddress.build());
				break;
			case 7:
				System.out.println("Wpisz nową datę dostarczenia przesyłki (DD-MM-RRRR)");
				String newShippingDate = scanner.nextLine();
				Update updateShippingDate = QueryBuilder.update("transport").setColumn("shippingDate", QueryBuilder.literal(newShippingDate)).whereColumn("id").isEqualTo(QueryBuilder.literal(id));
				session.execute(updateShippingDate.build());
				break;
			case 8:
				System.out.println("Wpisz nowy status przesyłki");
				String newStatus = scanner.nextLine();
				Update updateStatus = QueryBuilder.update("transport").setColumn("status", QueryBuilder.literal(newStatus)).whereColumn("id").isEqualTo(QueryBuilder.literal(id));
				session.execute(updateStatus.build());
				break;
			case 9:
				System.out.println("Wpisz nowe imię kierowcy");
				String newFirstName = scanner.nextLine();
				Update updateFirstName = QueryBuilder.update("firmaPrzewozowa", "transport").setField("driver", "firstName", QueryBuilder.literal(newFirstName)).whereColumn("id").isEqualTo(QueryBuilder.literal(id));
				session.execute(updateFirstName.build());
				break;
			case 10:
				System.out.println("Wpisz nowe nazwisko kierowcy");
				String newLastName = scanner.nextLine();
				Update updateLastName = QueryBuilder.update("firmaPrzewozowa", "transport").setField("driver", "lastName", QueryBuilder.literal(newLastName)).whereColumn("id").isEqualTo(QueryBuilder.literal(id));
				session.execute(updateLastName.build());
				break;
			case 11:
				System.out.println("Wpisz nowy numer prawa jazdy");
				Integer newDrivingLicenseNumber = scanner.nextInt();
				scanner.nextLine();
				Update updateDrivingLicenseNumber = QueryBuilder.update("firmaPrzewozowa", "transport").setField("driver", "drivingLicenseNumber", QueryBuilder.literal(newDrivingLicenseNumber)).whereColumn("id").isEqualTo(QueryBuilder.literal(id));
				session.execute(updateDrivingLicenseNumber.build());
				break;
		}
	}

	public void deleteFromTable(Scanner scanner) {
		System.out.println("Podaj id zlecenia");
		String id = scanner.nextLine();
		Delete delete = QueryBuilder.deleteFrom("transport").whereColumn("id").isEqualTo(QueryBuilder.literal(id));
		session.execute(delete.build());
		System.out.println("Zlecenie o id "+ id +" zostało usunięte");
	}

	public void selectFromTable() {
		Select query = QueryBuilder.selectFrom("transport").all();
		displayTransport(query);
	}

	public void selectById(Scanner scanner) {
		System.out.println("Podaj id aby wyszukać zlecenie:");
		String id = scanner.nextLine();
		Select query = QueryBuilder.selectFrom("transport").all().whereColumn("id").isEqualTo(QueryBuilder.literal(id));
		displayTransport(query);
	}

	private void displayTransport(Select query) {
		SimpleStatement statement = query.build();
		ResultSet resultSet = session.execute(statement);
		for (Row row : resultSet) {
			System.out.println("Zlecenie: ");
			System.out.println("id:  " + row.getString("id") );
			System.out.println("nazwa:  " + row.getString("name") );
			System.out.println("rodzaj:  " + row.getString("type") );
			System.out.println("waga (tony):  " + row.getDouble("weight") );
			System.out.println("adres nadania:  " + row.getString("postingAddress") );
			System.out.println("data nadania (DD-MM-RRRR): " + row.getString("postingDate") );
			System.out.println("adres dostarczenia:  " + row.getString("shippingAddress") );
			System.out.println("data dostarczenia (DD-MM-RRRR):  " + row.getString("shippingDate") );
			System.out.println("status " + row.getString("status") );
			UdtValue driver = row.getUdtValue("driver");
			System.out.println(" Imię kierowcy:  " + driver.getString("firstName") +  "\n Nazwisko kierowcy:  " + driver.getString("lastName")
					+ "\n Numer prawa jazdy:  " + driver.getInt("drivingLicenseNumber") );
			System.out.println();
		}
		System.out.println("Statement \"" + statement.getQuery() + "\" executed successfully");
	}

	public void selectByTypeAndWeight(Scanner scanner) {
		System.out.println("Podaj typ zlecenia");
		String type = scanner.nextLine();
		System.out.println("Podaj minimalną wagę w tonach");
		double weight = scanner.nextDouble();
		scanner.nextLine();
		Select query = QueryBuilder.selectFrom("transport").all().where(Relation.column("type").isEqualTo(QueryBuilder.literal(type)), Relation.column("weight").isGreaterThan(QueryBuilder.literal(weight))).allowFiltering();
		displayTransport(query);
	}

	public void selectCount(Scanner scanner){
		System.out.println("Podaj statuts");
		String type = scanner.nextLine();
		Select query = QueryBuilder.selectFrom("transport").all().whereColumn("status").isEqualTo(QueryBuilder.literal(type)).allowFiltering();
		SimpleStatement statement = query.build();
		ResultSet resultSet = session.execute(statement);
		System.out.println("Ilość zleceń o statusie " + type + " wynosi " + resultSet.all().size());
	}
}
