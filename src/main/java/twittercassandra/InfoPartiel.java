package twittercassandra;

import java.util.List;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import cassandra.ConnexionCas;
import cassandra.KsTable;
import jsonCassandra.JsonToCassandra;
import twitter4j.TwitterException;

public class InfoPartiel {
	public static String getMediaType(JsonObject tweet) {
		String type = "";
		try {
			// récupérer liste des hashtag puis pour chauqe media
//			
			System.out.println(tweet.get("id").getAsString());
			for (JsonElement unExMedia : tweet.get("extended_entities").getAsJsonObject().get("media")
					.getAsJsonArray()) {
				try {
					System.out.println(unExMedia.getAsJsonObject().get("type").getAsString());
					type=unExMedia.getAsJsonObject().get("type").getAsString();
				} catch (IndexOutOfBoundsException e) {
					e.printStackTrace();
					type= "non media";
				}
			}
		} catch (NullPointerException e) {
			System.err.println("non media");
			type="non media";
		}
		return type;
	}

	public static void main(String[] args) {

		/*************
		 * 
		 * 1. créer table avec la requête 2. alimenter la table avec la requête Pour
		 * format texte : ConvertirCQL.convertir(j.get("text").getAsString()) Pour
		 * format dateUTC : ConvertirCQL.toDateCQL(j.get("created_at").getAsString())
		 ************/
		Cluster c = ConnexionCas.connecter();
		Session session = ConnexionCas.session(c);
		try {

			/******************************
			 ********** Créer tables********
			 *****************************/

			KsTable.createTab(session, "ks", "tweet");

			/******************************
			 ********** Alimenter***********
			 *****************************/
			try {
				for (JsonObject j : JsonToCassandra.getJsons(session, "ks.tweets")) {
					System.out.println(j.get("id_str").getAsString());
						String queryAliment = "";
						String cql1 = "";
						String cql2 = "";
						String cql3 = "";
						String cql4 = "";
						String cql5 = "";
						cql1 += "(idtweet,name,contenu,createdDate,nombreLike,nombreRT,typemedia)";
						cql2 += " values('" + j.get("id_str").getAsString() + "'";
						cql3 += ",'" + j.get("user").getAsJsonObject().get("screen_name").getAsString();
						cql4 += "','" + ConvertirCQL.convertir(j.get("full_text").getAsString());
						cql5 += "','" + ConvertirCQL.toDateCQL(j.get("created_at").getAsString()) 
						        + "'," + j.get("favorite_count").getAsInt() + "," + j.get("retweet_count").getAsInt()
						        + ",'" +InfoPartiel.getMediaType(j) +"'" ;

						queryAliment = "insert into ks.tweet " + cql1 + cql2 + cql3 + cql4 + cql5 + ")";
						System.out.println(queryAliment);
						session.execute(queryAliment);
					}
				System.out.println("-----fini-----");

			} catch (Exception e) {
				System.err.println("[insert error] : " + e.getMessage());

				e.printStackTrace();
				// TODO: handle exception
			}

		} catch (Exception e) {
			System.err.println("[create error] : " + e.getMessage());
			e.printStackTrace();
			// TODO: handle exception
		}
	}
}
