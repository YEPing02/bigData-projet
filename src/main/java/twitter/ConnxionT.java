package twitter;

import java.util.List;

import twitter4j.FilterQuery;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

import cassandra.ConnexionCas;
import twitter4j.HashtagEntity;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.TwitterObjectFactory;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.Configuration;
import twitter4j.conf.ConfigurationBuilder;
import twittercassandra.ConvertirCQL;

public class ConnxionT {
	// ping
//	static String ConsumerKey = "hgC7hqv9W41uhFPPz1UXrt8IH";
//	static String ConsumerSecret = "m7U2hLR5nFZnnMK2SbyNgvOwYz9ANHHvdgaELvwhCdxDeLApDq";
//	static String AccessToken = "961215723765948416-5I1vJ9aJ2gQWeHlEKWXdioyLUP7RDLw";
//	static String AccessTokenSecret = "kHJzMdSTgT21Ltznq1DkDq8WGJCJifimHzlLd3Ny0YCCb";
////
	static String ConsumerKey = "L44OYh4Rhko29WqgHT2W0ZKbx";
	static String ConsumerSecret = "RUbCgIlBLBg0WVqsauPuAMeFTnn2okc2AHPDKVaiUnd3BEqP8K";
	static String AccessToken = "1226840516848967681-sZjSw0lquYIvCmJU94TTIg3B70zEAI";
	static String AccessTokenSecret = "t0R2aPXn7AhDWNQDQnxILU7rYmbZ31pCBAqasozOIHtNa";

	public static Configuration Tconnexion() {
		ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();
		configurationBuilder.setDebugEnabled(true).setJSONStoreEnabled(true).setOAuthConsumerKey(ConsumerKey)
				.setOAuthConsumerSecret(ConsumerSecret).setOAuthAccessToken(AccessToken)
				.setOAuthAccessTokenSecret(AccessTokenSecret);
		return configurationBuilder.build();
	}

	public static Twitter getTwitterInstance() {
		Twitter te = new TwitterFactory(Tconnexion()).getInstance();
		return te;
	}

	public static TwitterStream getTwitterStreamInstance() {
		return new TwitterStreamFactory(Tconnexion()).getInstance();
	}

	// stream
	public static void insertStream(TwitterStream ts, final String ksTab, final Session session) throws TwitterException {

		ts = ts.addListener(new StatusListener() {

			public void onException(Exception ex) {
				// TODO Auto-generated method stub
				ex.printStackTrace();
			}

			public void onStatus(Status s) {
				
				
				
				session.execute(ConvertirCQL.insertS(s, ksTab));
				
				
				
				System.out.println(s.getCreatedAt().toString() +" user : "+s.getUser().getScreenName()+" text : "+s.getText()  );
			}

			public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
				// TODO Auto-generated method stub
				System.out.println("Got a status deletion notice id:" + statusDeletionNotice.getStatusId());

			}

			public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
				// TODO Auto-generated method stub
				System.out.println("Got track limitation notice:" + numberOfLimitedStatuses);
			}

			public void onScrubGeo(long userId, long upToStatusId) {
				// TODO Auto-generated method stub

			}

			public void onStallWarning(StallWarning warning) {
				// TODO Auto-generated method stub

			}
		});

	};

	public static FilterQuery filterOnAtUser() {
		FilterQuery tweetFilterQuery = new FilterQuery();
		tweetFilterQuery.track(new String[] { "@LCI", });
		return tweetFilterQuery;
	}

	public static void main(String[] args) throws TwitterException {

		// connexion

		Twitter twitter = ConnxionT.getTwitterInstance();
		//
		long idsta = 1233094452069257221L;
		Status s = twitter.showStatus(idsta);
		System.out.println(TwitterObjectFactory.getRawJSON(s));
		Cluster cc = ConnexionCas.connecter();
		Session ses = ConnexionCas.session(cc);

		String cql1 = "";
		String cql2 = "";
		String cql3 = "";
		long id = s.getId();
		String strid = String.valueOf(id);
		String twjson = TwitterObjectFactory.getRawJSON(s).toString();
		cql1 += "(idtweet,tw)";
		cql2 += " values('" + strid + "'";
		cql3 += ",'" + ConvertirCQL.convertir(twjson) + "'";

		String cql = "insert into " + "ks.tweets" + cql1 + cql2 + cql3 + ");";
		System.out.println(cql);

		ses.execute(cql);
		// List<Status> status = twitter.getHomeTimeline();
//		List<Status> statusUser = twitter.getUserTimeline(26110930);

//		try {
//			for (Status s : statusUser) {
//				System.out.println(s.getUser().getScreenName());
//				IDs ids=twitter.getFollowersIDs(26110930,1);
//				for(long id : ids.getIDs()) {
//					User u=twitter.showUser(id);
//					System.out.println(u.getScreenName());
//				}
//				System.out.println("-------");
//			}
//		} catch (TwitterException e) {
//			System.err.println(e.getErrorMessage());
//			System.err.println(e.getRateLimitStatus());
//
////			System.err.println(e.);
//		}

//		 hashtag
//		try {
//		for (Status s : statusUser) {
//			System.out.println(s.getId());
//			HashtagEntity[] he =s.getHashtagEntities();
//			for(HashtagEntity h: he) {
//				System.out.println(h.getText());
//			}
//			System.out.println("-------");
//		}
//	} catch (Exception e) {
//		e.printStackTrace();
//	}
//		
//		try {
//			// faut pré-créer un txt
//			File writename = new File(".\\resultat\\output.txt"); //
//			writename.createNewFile();
//			BufferedWriter out = new BufferedWriter(new FileWriter(writename));
//			for (Status s : statusUser) {
//				String json = TwitterObjectFactory.getRawJSON(s);
//				System.out.println(json);
//				out.write(json);
//			}
//			out.flush(); //
//			out.close(); //
//		} catch (Exception e) {
//			e.printStackTrace();
//		}

	}
}
