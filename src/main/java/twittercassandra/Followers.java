package twittercassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

import cassandra.ConnexionCas;
import cassandra.KsTable;

public class Followers {
	public static void main(String[] args) {

		// connexion
		Cluster cluster = ConnexionCas.connecter();
		Session session = ConnexionCas.session(cluster);
		KsTable.dropTab(session, "ks.followers");
		KsTable.createKS(session, "ks");
		KsTable.createTabFollower(session, "ks", "followers");
		//
		
	
		
	}
}