package jsonCassandra;

import java.util.ArrayList;
import java.util.List;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import cassandra.ConnexionCas;
import cassandra.KsTable;

public class JsonToCassandra {

	// get liste string json

	// get all tweets sous format json.
	public static ArrayList<JsonObject> getJsons(Session session, String ksTab) {
		// get all lignes
		List<Row> lsJsonR = session.execute("Select twjson from " + ksTab + ";").all();

		ArrayList<JsonObject> lsJson = new ArrayList<JsonObject>();
		for (Row r : lsJsonR) {
			JsonObject json = toJson(r.getString(0));
			lsJson.add(json);
		}
		return lsJson;
	}

	public static ArrayList<String> getHashtagsUnTweet(JsonObject jsontw) {
		ArrayList<String> lstHashtags = new ArrayList<String>();
		// récupérer liste des hashtag puis pour chauqe hashtag
		for (JsonElement unHashtag : jsontw.get("entities").getAsJsonObject().get("hashtags").getAsJsonArray()) {
			try {
				lstHashtags.add(unHashtag.getAsJsonObject().get("text").getAsString());
			} catch (IndexOutOfBoundsException e) {
				e.printStackTrace();
			}
		}
		return lstHashtags;
	}

	public static ArrayList<String> getMentionedUserNameUnTweet(JsonObject jsontw) {
		ArrayList<String> lstMuser = new ArrayList<String>();
		// récupérer liste des hashtag puis pour chauqe hashtag
		for (JsonElement unMUser : jsontw.get("entities").getAsJsonObject().get("user_mentions").getAsJsonArray()) {
			try {
				lstMuser.add(unMUser.getAsJsonObject().get("screen_name").getAsString());
			} catch (IndexOutOfBoundsException e) {
				e.printStackTrace();
			}
		}
		return lstMuser;
	}

	private static JsonObject toJson(String jsonstr) {

		JsonParser jp = new JsonParser();
		JsonObject jo = jp.parse(jsonstr).getAsJsonObject();
		return jo;
	}

	public static ArrayList<String> getJsonStrs(Session session, String ksTab) {
		// get all lignes
		List<Row> lsJsonR = session.execute("Select twjson from " + ksTab + ";").all();

		ArrayList<String> lsStr = new ArrayList<String>();
		for (Row r : lsJsonR) {
			lsStr.add(r.toString());
		}
		return lsStr;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// connexion
		Cluster cluster = ConnexionCas.connecter();
		Session session = ConnexionCas.session(cluster);
		// KsTable.dropKS(session, "keyspacelci");
		KsTable.createKS(session, "ks");
//				KsTable.createTabAllInfo(session, "ks", "tweets");
		//
		ArrayList<JsonObject> lsJson = getJsons(session, "ks.tweets");

		for (JsonObject json : lsJson) {
			System.out.println(json.get("id_str").getAsString());
			for (String u : getMentionedUserNameUnTweet(json)) {
				try {
					
			
				System.out.println(u);
				} catch (Exception e) {
					System.err.println("non mentioned");// TODO: handle exception
				}
			}

		}

//					

	}

}
