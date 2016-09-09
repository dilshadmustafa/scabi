/**
 * @author Dilshad Mustafa
 * Copyright (c) Dilshad Mustafa
 * All Rights Reserved.
 * Created 11-Feb-2016
 * File Name : DDBOld.java
 */

/**
Copyright (c) Dilshad Mustafa 2016. All Rights Reserved.

User and Developer License

1. You can use this Software for both Personal use as well as Commercial use, 
with or without paying fee to Dilshad Mustafa. Please read fully below for the 
terms and conditions. You may use this Software only if you comply fully with 
all the terms and conditions of this License. 

2. If you want to redistribute this Software, you should redistribute only the 
original source code from Dilshad Mustafa's project and/or the compiled object 
binary form of the original source code and you should ensure to bundle and 
display this original license text and Dilshad Mustafa's copyright notice along 
with it as well as in each source code file of this Software. 

3. If you want to embed this Software within your work, you should embed only the 
original source code from Dilshad Mustafa's project and/or the compiled object 
binary form of the original source code and you should ensure to bundle and display 
this original license text and Dilshad Mustafa's copyright notice along with it as 
well as in each source code file of this Software. 

4. You should not modify this Software source code and/or its compiled object binary 
form in any way.

5. You should not redistribute any modified source code of this Software and/or 
its compiled object binary form with any changes, additions, enhancements, 
updates or modifications. You should not redistribute any modified works of this 
Software. You should not create and/or redistribute any straight forward 
translation and/or implementation of this Software source code to same and/or 
another programming language, either partially or fully. You should not redistribute 
embedded modified versions of this Software source code and/or its compiled object 
binary in any form, both within as well as outside your organization, company, 
legal entity and/or individual. 

6. You should not embed any modification of this Software source code and/or its compiled 
object binary form in any way, either partially or fully.

7. Under differently named or renamed software, you should not redistribute this 
Software and/or any modified works of this Software, including its source code 
and/or its compiled object binary form. Under your name or your company name or 
your product name, you should not publish this Software, including its source code 
and/or its compiled object binary form, modified or original. 

8. You agree to use the original source code from Dilshad Mustafa's project only
and/or the compiled object binary form of the original source code.

9. You accept and agree fully to the terms and conditions of this License of this 
software product, under same software name and/or if it is renamed in future.

10. This software is created and programmed by Dilshad Mustafa and Dilshad holds the 
copyright for this Software and all its source code. You agree that you will not infringe 
or do any activity that will violate Dilshad's copyright of this software and all its 
source code.

11. The Copyright holder of this Software reserves the right to change the terms 
and conditions of this license without giving prior notice.

12. THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
   EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
   MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
   NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHOR OR COPYRIGHT HOLDER
   BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
   ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
   CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
   SOFTWARE.

*/

package com.dilmus.dilshad.scabi.deprecated;

import java.util.ArrayList;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dilmus.dilshad.scabi.common.DScabiException;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MapReduceCommand;
import com.mongodb.MapReduceOutput;
import com.mongodb.MongoClient;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

/**
 * @author Dilshad Mustafa
 *
 */
public class DDBOld {

	final static Logger log = LoggerFactory.getLogger(DDBOld.class);
	
	private MongoClient m_mongo = null;
	private MongoDatabase m_mongodb = null;
	private DB m_db = null;
	
	private String m_dbHost = null;
	private String m_dbPort = null;
	private String m_dbName = null;
	
	public DDBOld(String dbHost, String dbPort, String dbName) {
		m_mongo = new MongoClient(dbHost, Integer.parseInt(dbPort));
		m_mongodb = m_mongo.getDatabase(dbName);
		m_db = new DB(m_mongo, dbName);
		
		m_dbHost = dbHost;
		m_dbPort = dbPort;
		m_dbName = dbName;
		
		m_mongo.setWriteConcern(WriteConcern.ACKNOWLEDGED);
		// ?? m_mongo.setReadPreference(ReadPreference.primaryPreferred());
	}
	
	public DB getDB() {
		return m_db;
	}
	
	public MongoDatabase getDatabase() {
		return m_mongodb;
	}
	
	/* Keep it for future for use with MongoDatabase mongodb
	public boolean collectionExists(String tableName) {
	    MongoIterable<String> collectionNames = mongodb.listCollectionNames();
	    for (String name : collectionNames) {
	        if (name.equalsIgnoreCase(tableName)) {
	            return true;
	        }
	    }
	    return false;
	}
	*/
	
	public boolean tableExists(String tableName) {
	    return m_db.collectionExists(tableName);
	}
	
	public DTableOld getTable(String tableName) throws DScabiException {
		if (null == tableName) {
			throw new DScabiException("Table name is null", "DBD.GTE.1");
		}
		if (false == m_db.collectionExists(tableName)) {
			throw new DScabiException("Table name doesn't exist : " + tableName, "DBD.GTE.2");
		}
		DBCollection table = m_db.getCollection(tableName);
		return new DTableOld(this, table);
	}

	public DTableOld createTable(String tableName) throws DScabiException {
		DBCollection table = null;
		if (null == tableName) {
			throw new DScabiException("Table name is null", "DBD.CTE.1");
		}
		if (false == m_db.collectionExists(tableName)) {
			log.debug("Table doesn't exist : {}. So creating", tableName);
			table = m_db.createCollection(tableName, null);
		}
		else
			table = m_db.getCollection(tableName);
		return new DTableOld(this, table);
	}

	public ArrayList<String> fieldNames(DTableOld table) throws DScabiException {
		if (null == table) {
			throw new DScabiException("Table is null", "DBD.FNS.1");
		}
		log.debug("fieldNamesUsingFindOne() table.count() is {}", table.count());
		if (table.count() <= 0) {
			return null;
		}
		String map = "function() { for (var key in this) { emit(key, null); } }";
		String reduce = "function(key, stuff) { return null; }";
			
		MapReduceCommand cmd = new MapReduceCommand(table.getCollection(), map, reduce,
				   	     null, MapReduceCommand.OutputType.INLINE, null);
		MapReduceOutput out = table.getCollection().mapReduce(cmd);
		//if 4th param output collection name is used above, String s = out.getOutputCollection().distinct("_id").toString();
		//if 4th param output collection name is used above, System.out.println("out.getOutputCollection().distinct " + s);
		ArrayList<String> fieldNames = new ArrayList<String>();
		for (DBObject o : out.results()) {
		    log.debug("fieldNames() Key, value is : {}", o.toString());
		    log.debug("fieldNames() Key name is : {}", o.get("_id").toString());
		    if (false == o.get("_id").toString().equals("_id"))
		    	fieldNames.add(o.get("_id").toString());
		}			
		return fieldNames;
	
	}

	public ArrayList<String> fieldNamesUsingFindOne(DTableOld table) throws DScabiException {
		if (null == table) {
			throw new DScabiException("Table is null", "DBD.FNU.1");
		}
		log.debug("fieldNamesUsingFindOne() table.count() is {}", table.count());
		if (table.count() <= 0) {
			return null;
		}
		DBObject out = table.getCollection().findOne();
		ArrayList<String> fieldNames = new ArrayList<String>();
		Set<String> st = out.keySet();
		for (String s : st) {
		    log.debug("fieldNamesUsingFindOne() value is : {}", out.get(s));
		    log.debug("fieldNamesUsingFindOne() Key name is : {}", s);
		    if (false == s.equals("_id"))
		    	fieldNames.add(s);
		}			
		return fieldNames;
	}
	
	
	public int close() {
		m_mongo.close();
		return 0;
	}
	
}
