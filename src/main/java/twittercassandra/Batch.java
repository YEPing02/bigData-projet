package twittercassandra;

import java.util.List;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import cassandra.ConnexionCas;
import cassandra.KsTable;
import twitter4j.TwitterException;

public class Batch {
	public static void main(String[] args) {
		// établir la connexion à partir des méthodes que l'on a définies
		Cluster cluster = ConnexionCas.connecter();
		// établir une seesion pour exécution CQL
		Session session = ConnexionCas.session(cluster);

		// création keyspace et table
		KsTable.createKS(session, "ks");
		KsTable.createTabAllInfo(session, "ks", "tweets");

		// alimentation
		try {
			for (int i = 1; i < 100; i++) {
				// une méthode pour prendre les info en Json string
				List<String> insertT = ConvertirCQL.insertAll(i, "ks.tweets");

				// exécution de CQL pour alimentation
				for (String cqlTweet : insertT) {
					// insert into tweet table
					try {
						session.execute(cqlTweet);
					} catch (Exception e) {
						e.printStackTrace();

					}
				}
			}
			cluster.close();
		} catch (

		TwitterException e) {
			System.err.println(e.getErrorMessage());
			System.err.println(e.getRateLimitStatus());
		}

	}
}
