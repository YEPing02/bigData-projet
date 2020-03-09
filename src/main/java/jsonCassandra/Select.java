package jsonCassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.google.gson.JsonObject;

import cassandra.ConnexionCas;
import twittercassandra.ConvertirCQL;

public class Select {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Cluster c = ConnexionCas.connecter();
		Session session = ConnexionCas.session(c);
		try {
			for (JsonObject j : JsonToCassandra.getJsons(session, "ks.twitterstreaming")) {
				String cql4 = ConvertirCQL.convertir(j.get("text").getAsString());
if (cql4.contains("de devenir instanta")) {

				System.out.println(cql4);}
break;
			}
			System.out.println("-----fini-----");
			
			
			
		} catch (Exception e) {
			System.err.println("[insert error] : " + e.getMessage());

			e.printStackTrace();
			// TODO: handle exception
		}
	}

}
