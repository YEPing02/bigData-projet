package twittercassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.google.gson.JsonObject;

import cassandra.ConnexionCas;
import cassandra.KsTable;
import jsonCassandra.JsonToCassandra;

public class Hashtag {
	public static void main(String[] args) {

		/*************
		 * 1. créer table avec la requête 
		 * 2. alimenter la table avec la requête format
		 * texte : ConvertirCQL.convertir(j.get("text").getAsString()) Pour format
		 * dateUTC : ConvertirCQL.toDateCQL(j.get("created_at").getAsString())
		 ************/
		Cluster c = ConnexionCas.connecter();
		Session session = ConnexionCas.session(c);
		try {

			/******************************
			 ********** Créer tables********
			 *****************************/

			KsTable.createTabHashtag(session, "ks", "hashtag");

			/******************************
			 ********** Alimenter***********
			 *****************************/
			try {
				for (JsonObject j : JsonToCassandra.getJsons(session, "ks.tweets")) {
					for (String hashtag : JsonToCassandra.getHashtagsUnTweet(j)) {
						String queryAliment = "";
						String cql1 = "";
						String cql2 = "";
						String cql3 = "";
						cql1 += "(idtweet,hashtag)";
						cql2 += " values('" + j.get("id_str").getAsString() + "'";
						cql3 += ",'" + hashtag + "'";
						queryAliment = "insert into ks.hashtag " + cql1 + cql2 + cql3 + ")";

						System.out.println(queryAliment);
						session.execute(queryAliment);
					}
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
		}
	}
}
