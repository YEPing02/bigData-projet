package cassandra;

import java.util.List;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;

public class ConnexionCas {
	static String ip = "127.0.0.1";
	static int port = 9042;
	static String keySpace = "demo";

	// connection
	public static Cluster connecter() {
		return Cluster.builder().addContactPoint(ip).withPort(port).build();
	}

	// session
	public static Session session(Cluster cluster) {
		return connecter().connect();
	}

	public static void main(String[] args) {

		// connexion
		Cluster cluster = ConnexionCas.connecter();
		Session s = session(cluster);
		//

		Select slc = QueryBuilder.select().all().from("demo", "users");
		Insert ist = QueryBuilder.insertInto("demo", "users").value("lastename", "Wang").value("age", 25)
				.value("city", "Toulouse").value("email", "yepingisa@gmail.com").value("firstname", "Ping");
		String ist1 = "insert into demo.users (lastename,age,city,email,firstname) values('aersilang',26,'Toulouse','123@mail.com','Abudu')";

		s.execute(ist1);
		List<Row> lsR = s.execute(slc).all();
		for (Row r : lsR) {
			System.out.println(r.getString("lastename"));
		}
//		Metadata metadata = cluster.getMetadata();
//		for (Host host : metadata.getAllHosts()) {
//			System.out.println("------" + host.getAddress());
//		}
//		System.out.println("======================");
//		for (KeyspaceMetadata keyspaceMetadata : metadata.getKeyspaces()) {
//			System.out.println("--------" + keyspaceMetadata.getName());
//		}
		cluster.close();
	}
}
