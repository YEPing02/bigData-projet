package twittercassandra;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import twitter.ConnxionT;
import twitter4j.IDs;
import twitter4j.Paging;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;
import twitter4j.User;

public class ConvertirCQL {
	static Twitter twitter = ConnxionT.getTwitterInstance();
	static String user = "LCI";
	static int idLCI = 26110930;
	static long cptInsert = 1;

	public static List<String> insert(int pageno) throws TwitterException {
		List<String> cqlTweets = new ArrayList<String>();
		List<Status> statuses;
		Paging pageTweet = new Paging(pageno, 100);
		statuses = twitter.getUserTimeline(user, pageTweet);

		for (Status s : statuses) {
			String cql1 = "";
			String cql2 = "";
			String cql3 = "";
			String cql4 = "";
			String cql5 = "";
			long id = s.getId();
			String strid = String.valueOf(id);
			cql1 += "(idtweet,name,contenu,createdDate,nombreLike,nombreRT)";
			cql2 += " values('" + strid + "'";
			cql3 += ",'" + s.getUser().getName();
			cql4 += "','" + convertir(s.getText());
			cql5 += "','" + sdf.format(s.getCreatedAt()) + "'," + s.getFavoriteCount() + "," + s.getRetweetCount();

			String cql = "insert into ks.tweet " + cql1 + cql2 + cql3 + cql4 + cql5 + ")";
			System.out.println("-----" + cptInsert + "-----");
			cptInsert++;
			System.out.println(cql);
			cqlTweets.add(cql);
			sleep(10);
		}
		return cqlTweets;
	}

	public static List<String> insertAll(int pageno, String ksTab) throws TwitterException {
		List<String> cqlTweets = new ArrayList<String>();
		List<Status> statuses;
		Paging pageTweet = new Paging(pageno, 200);
		statuses = twitter.getUserTimeline(user, pageTweet);

		for (Status s : statuses) {
			String cql1 = "";
			String cql2 = "";
			String cql3 = "";
			long id = s.getId();
			String strid = String.valueOf(id);
			String twjson = TwitterObjectFactory.getRawJSON(s).toString();
			cql1 += "(idtweet,twjson)";
			cql2 += " values('" + strid + "'";
			cql3 += ",'" + convertir(twjson) + "'";

			String cql = "insert into " + ksTab + cql1 + cql2 + cql3 + ");";
			System.out.println("-----" + cptInsert + "-----");
			cptInsert++;
			System.out.println(cql);
			cqlTweets.add(cql);
			sleep(10);
		}
		return cqlTweets;
	}

	public static String insertS(Status s, String ksTab) {
		String cql1 = "";
		String cql2 = "";
		String cql3 = "";
		long id = s.getId();
		String strid = String.valueOf(id);
		String twjson = TwitterObjectFactory.getRawJSON(s).toString();
		cql1 += "(idtweet,twjson)";
		cql2 += " values('" + strid + "'";
		cql3 += ",'" + convertir(twjson) + "'";
		String cql = "insert into " + ksTab + cql1 + cql2 + cql3 + ");";
		System.out.println("-----" + cptInsert + "-----");
		cptInsert++;
		System.out.println("inséré");
		sleep(10);
		return cql;
	}

	//
	static Paging page = new Paging();

	static void sleep(long ms) {
		try {
			Thread.sleep(ms);
		} catch (InterruptedException ex) {
			Thread.currentThread().interrupt();
		}
	}

	public static ArrayList<String> followers() throws TwitterException {
		ArrayList<String> followers = new ArrayList<String>();
		IDs ids = twitter.getFollowersIDs(idLCI, -1);
		int i = 0;
		for (long id : ids.getIDs()) {
			if (i < 500) {
				List<Status> status;
				page.count(1);
				status = twitter.getUserTimeline(user, page);
				String cql1 = "";
				User user = twitter.showUser(id);
				cql1 += "(id,screenname,name,creatDate,nbTweets,nbFolowers,location,geo)";
				String cql = cql1 + " values('" + id + "','" + user.getScreenName() + "','" + convertir(user.getName())
						+ "','" + sdf.format(user.getCreatedAt()) + "'," + user.getStatusesCount() + ","
						+ user.getFollowersCount() + ",'" + convertir(user.getLocation()) + "','";
				if (status.get(0).getGeoLocation() != null) {
					cql += status.get(0).getGeoLocation().getLatitude() + "+"
							+ status.get(0).getGeoLocation().getLongitude();
				}
				cql += "')";
				System.out.println(i + "  -----" + cql);
				followers.add(cql);
				sleep(100);
				i++;
			} else {
				break;
			}
		}
		return followers;
	}

	// adaptation of date
	static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

	public static String toDateCQL(String TwitterDate) throws ParseException {
		SimpleDateFormat sf = new SimpleDateFormat("EEE MMM dd HH:mm:ss ZZZZZ yyyy", Locale.ENGLISH);
		// set English pour comprendre les mot anglais

		return sdf.format(sf.parse(TwitterDate));
	}

	// convertion de ' à ''
	public static String convertir(String contenu) {
		String res = "";
		String[] listC = contenu.split("");
		for (String str : listC) {
			if (str.equals("'")) {
				str = "''";
			}
			res += str;
		}
		return res;
	}

	public static void main(String[] args) {
		try {
			System.out.println(ConvertirCQL.toDateCQL("Fri Feb 28 12:14:10 +0000 2020"));
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}