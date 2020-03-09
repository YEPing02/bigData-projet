package cassandra;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;

public class KsTable {
	static String nomKs;

	// create keyspace if not exists
	public static void createKS(Session s, String nomKS) {

		setNomKs(nomKS);
		String createKeySpaceCQL = "create keyspace if not exists " + nomKS + " with "
				+ "replication={'class':'SimpleStrategy','replication_factor':1}";
		s.execute(createKeySpaceCQL);
	}

	public static String getNomKs() {
		return nomKs;
	}

	public static void setNomKs(String nomKs) {
		KsTable.nomKs = nomKs;
	}

	// create table tweets
	public static void createTab(Session s, String nomKS, String nomTab) {
		String createTableTweet = "create table if not exists " + nomKS + "." + nomTab + "("
				+ "idtweet varchar primary key," + "name varchar," + "contenu varchar," + "createdDate date,"
				+ "nombreLike int," + "nombreRT int, typemedia varchar)";
		s.execute(createTableTweet);
	}

	public static void createTabHashtag(Session s, String nomKS, String nomTab) {
		String createTableTweet = "create table if not exists " + nomKS + "." + nomTab + "("
				+ "idtweet varchar," + "hashtag varchar, primary key (idtweet,hashtag))";
		s.execute(createTableTweet);
	}

	public static void createTabMentionedUser(Session s, String nomKS, String nomTab) {
		String createTableTweet = "create table if not exists " + nomKS + "." + nomTab + "("
				+ "idtweet varchar," + "muser varchar, primary key (idtweet,muser))";
		s.execute(createTableTweet);
	}
	
	public static void createTabAllInfo(Session s, String nomKS, String nomTab) {
		String createTableTweet = "create table if not exists " + getNomKs() + "." + nomTab + "("
				+ "idtweet varchar primary key," + "twjson text);";
		s.execute(createTableTweet);
	}

	public static void createTabSt(Session s, String nomKS, String nomTab) {
		String createTableTweet = "create table if not exists " + getNomKs() + "." + nomTab + "("
				+ "idtweet varchar primary key," + "twjson text);";
		s.execute(createTableTweet);
	}
	
	public static void createTabFollowerAllInfo(Session s, String nomKS, String nomTab) {
		String createTableTweet = "create table if not exists " + getNomKs() + "." + nomTab + "("
				+ "idfollower varchar primary key," + "userjson text);";
		s.execute(createTableTweet);
	}
	
	public static void createTabFollower(Session s, String nomKS, String nomTab) {
		String createTableTweet = "create table if not exists " + getNomKs() + "." + nomTab + "("
				+ "id varchar primary key," + "screenname text,name text,creatDate date,nbTweets int,nbFolowers int, location text,geo text );";
		s.execute(createTableTweet);
	}

	public static void dropKS(Session s, String nomKS) {

		String createKeySpaceCQL = "drop keyspace " + nomKS + ";";
		s.execute(createKeySpaceCQL);
	}

	public static void dropTab(Session s, String nomKsTab) {
		String createKeySpaceCQL = "drop table " + nomKsTab+";";
		s.execute(createKeySpaceCQL);
	}

	public static void main(String[] args) {
	}
}
