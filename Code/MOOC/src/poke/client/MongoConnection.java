package poke.client;

import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import com.mongodb.WriteConcern;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.DBCursor;
import com.mongodb.ServerAddress;
import java.util.*;

public class MongoConnection {

	public static void main(String[] args)
	{ try{
		MongoClient mongo=new MongoClient("localhost",27017);
		DB db=mongo.getDB("cmpe275");
		DBCollection d=db.getCollection("users");
		BasicDBObject bo=new BasicDBObject("name","vk").append("uname", "val").append("pass"," lkval");
		d.insert(bo);
		

		
	}
	catch(Exception e)
	{
		e.printStackTrace();}
	}
}
