/**
 * @author Dilshad Mustafa
 * Copyright (c) Dilshad Mustafa
 * All Rights Reserved.
 * @since 10-Mar-2016
 * File Name : DDB.java
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

5. You should not redistribute any modified source code of this Software and/or its 
compiled object binary form with any changes, additions, enhancements, updates or 
modifications, any modified works of this Software, any straight forward translation 
and/or implementation to same and/or another programming language and embedded modified 
versions of this Software source code and/or its compiled object binary in any form, 
both within as well as outside your organization, company, legal entity and/or individual. 

6. You should not embed any modification of this Software source code and/or its compiled 
object binary form in any way, either partially or fully.

7. You should not redistribute this Software and/or any modified works of this 
Software, including its source code and/or its compiled object binary form, under 
differently named or renamed software. You should not publish this Software, including 
its source code and/or its compiled object binary form, modified or original, under 
your name or your company name or your product name. You should not sell this Software 
to any party, organization, company, legal entity and/or individual.

8. You agree fully to the terms and conditions of this License of this software product, 
under same software name and/or if it is renamed in future.

9. This software is created and programmed by Dilshad Mustafa and Dilshad holds the 
copyright for this Software and all its source code. You agree that you will not infringe 
or do any activity that will violate Dilshad's copyright of this software and all its 
source code.

10. The Copyright holder of this Software reserves the right to change the terms 
and conditions of this license without giving prior notice.

11. THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
   EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
   MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
   NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHOR OR COPYRIGHT HOLDER
   BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
   ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
   CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
   SOFTWARE.

*/

package com.dilmus.dilshad.scabi.db;

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
import com.mongodb.client.MongoIterable;

/**
 * @author Dilshad Mustafa
 *
 */
public class DDB {

	final static Logger log = LoggerFactory.getLogger(DDB.class);
	
	private MongoClient m_mongo = null;
	private MongoDatabase m_mongodb = null;
	
	private String m_dbHost = null;
	private String m_dbPort = null;
	private String m_dbName = null;
	
	public DDB(String dbHost, String dbPort, String dbName) {
		m_mongo = new MongoClient(dbHost, Integer.parseInt(dbPort));
		m_mongodb = m_mongo.getDatabase(dbName);		
		
		m_dbHost = dbHost;
		m_dbPort = dbPort;
		m_dbName = dbName;
		
		m_mongo.setWriteConcern(WriteConcern.ACKNOWLEDGED);
		// ?? m_mongo.setReadPreference(ReadPreference.primaryPreferred());
	}
	
	public MongoDatabase getDB() {
		return m_mongodb;
	}

	public MongoDatabase getDatabase() {
		return m_mongodb;
	}
	
	public boolean tableExists(String tableName) {
	    MongoIterable<String> collectionNames = m_mongodb.listCollectionNames();
	    for (String name : collectionNames) {
	        if (name.equalsIgnoreCase(tableName)) {
	            return true;
	        }
	    }
	    return false;
	}
	
	public DTable getTable(String tableName) throws DScabiException {
		if (null == tableName) {
			throw new DScabiException("Table name is null", "DBD.GTE.1");
		}
		if (false == tableExists(tableName)) {
			throw new DScabiException("Table name doesn't exist : " + tableName, "DBD.GTE.2");
		}
		MongoCollection<?> table = m_mongodb.getCollection(tableName);
		return new DTable(this, table);
	}

	public DTable createTable(String tableName) throws DScabiException {
		MongoCollection<?> table = null;
		if (null == tableName) {
			throw new DScabiException("Table name is null", "DBD.CTE.1");
		}
		if (false == tableExists(tableName)) {
			log.debug("Table doesn't exist : {}. So creating", tableName);
			m_mongodb.createCollection(tableName);
			return getTable(tableName);
		}
		else {
			//table = m_mongodb.getCollection(tableName);
			throw new DScabiException("Table already exists, table name : " + tableName, "DDB.CTE.1");
		}
		//return new DTable(this, table);
	}

	public int close() {
		m_mongo.close();
		return 0;
	}
	
}
