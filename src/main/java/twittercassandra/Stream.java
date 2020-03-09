package twittercassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import cassandra.ConnexionCas;
import cassandra.KsTable;
import twitter.ConnxionT;
import twitter4j.TwitterException;
import twitter4j.TwitterStream;

import twitter4j.FilterQuery;
public class Stream {
// get stream data
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		//
		Cluster cluster= ConnexionCas.connecter();
		Session session =ConnexionCas.session(cluster);
		KsTable.createKS(session, "ks");
		KsTable.createTabSt(session, "ks", "twitterstreaming");	
		TwitterStream ts=ConnxionT.getTwitterStreamInstance();
		FilterQuery qf = ConnxionT.filterOnAtUser();
		
		
		try {
			ConnxionT.insertStream(ts, "ks.twitterstreaming", session);
			ts.filter(qf);
		} catch (TwitterException e) {
			e.printStackTrace();
		}

	}

}
