package jsonCassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.schemabuilder.Create;
import com.google.gson.JsonObject;

import cassandra.ConnexionCas;
import twitter4j.Query;
import twittercassandra.ConvertirCQL;

public class SetTable {
	public static void main(String[] args) {
		
		
		/*************
		 * 
		 * 1. créer table avec la requête
		 * 2. alimenter la table avec la requête
		 * Pour format texte : ConvertirCQL.convertir(j.get("text").getAsString())
	 	 * Pour format dateUTC : ConvertirCQL.toDateCQL(j.get("created_at").getAsString())
		 * ************/
		Cluster c = ConnexionCas.connecter();
		Session session = ConnexionCas.session(c);
		try {

			/******************************
			 **********Créer tables********
			 *****************************/
		
			String queryCreation = "create table if not exists ks.tweetRef(" + "idtweet varchar primary key, " 
					+ "nameUser varchar,"
					+ "contenu varchar," + "createdDate date," + "nombreLike int)";
			System.out.println(queryCreation);
			session.execute(queryCreation);
			
			
			

			/******************************
			 **********Alimenter*********** 
			 *****************************/
			try {
				for (JsonObject j : JsonToCassandra.getJsons(session, "ks.twitterstreaming")) {
					String queryAliment="";
					
					String cql1 = "";
					String cql2 = "";
					String cql3 = "";
					String cql4 = "";
					String cql5 = "";
					cql1 += "(idtweet,nameUser,contenu,createdDate,nombreLike)";
					cql2 += " values('" + j.get("id_str").getAsString() + "'";
					cql3 += ",'" + j.get("user").getAsJsonObject().get("screen_name").getAsString();
					cql4 += "','" + ConvertirCQL.convertir(j.get("text").getAsString());
					cql5 += "','" + ConvertirCQL.toDateCQL(j.get("created_at").getAsString()) + "'" + ","
							+ j.get("favorite_count").getAsInt() ;

					queryAliment = "insert into ks.tweetRef " + cql1 + cql2 + cql3 + cql4 + cql5 + ")";


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
