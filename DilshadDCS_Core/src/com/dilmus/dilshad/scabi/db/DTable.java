/**
 * @author Dilshad Mustafa
 * Copyright (c) Dilshad Mustafa
 * All Rights Reserved.
 * Created 10-Mar-2016
 * File Name : DTable.java
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

package com.dilmus.dilshad.scabi.db;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Set;

import org.bson.BasicBSONObject;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dilmus.dilshad.scabi.common.DMJson;
import com.dilmus.dilshad.scabi.common.DScabiException;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MapReduceCommand;
import com.mongodb.MapReduceOutput;
import com.mongodb.WriteResult;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MapReduceIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;

/**
 * @author Dilshad Mustafa
 *
 */
public class DTable {

	final static Logger log = LoggerFactory.getLogger(DTable.class);
	
	private DDB m_ddb = null;
	private MongoCollection<Document> m_table = null;
	
	private String m_tableName;
	private boolean m_firstTime;
	private LinkedList<String> m_fieldNames;
	
	private DResultSet m_dcursor = null;
	
	public DTable(DDB ddb, MongoCollection table) {
		m_ddb = ddb;
		m_table = table;
		m_tableName = table.getNamespace().getCollectionName();
		m_firstTime = true;
		m_fieldNames = null;
		
		m_dcursor = new DResultSet();
		
	}

	public DTable(DDB ddb, String tableName) throws DScabiException {
		m_ddb = null;
		m_table = null;
		m_tableName = null;
		m_firstTime = true;
		m_fieldNames = null;
		
		MongoDatabase db = ddb.getDatabase();
		if (false == ddb.tableExists(tableName)) {
			throw new DScabiException("Table name doesn't exist : " + tableName, "DBT.DBT.1");
		}
		m_table = db.getCollection(tableName);
		m_tableName = tableName;
		m_ddb = ddb;

		m_dcursor = new DResultSet();

	}
	
	public DTable(DDB ddb) {
		m_ddb = ddb;
		m_table = null;
		m_tableName = null;
		m_firstTime = true;
		m_fieldNames = null;

		m_dcursor = new DResultSet();

	}
	
	public int setTableName(String tableName) throws DScabiException {
		m_tableName = null;
		m_firstTime = true;
		m_fieldNames = null;
		m_table = null;
		
		MongoDatabase db = m_ddb.getDatabase();
		if (false == m_ddb.tableExists(tableName)) {
			throw new DScabiException("Table name doesn't exist : " + tableName, "DBT.STN.1");
		}
		m_table = db.getCollection(tableName);
		m_tableName = tableName;
		return 0;
	}
	
	public long count() {
		return m_table.count();
	}

	public LinkedList<String> fieldNames() throws DScabiException {
		if (null == m_tableName) {
			throw new DScabiException("Table name is null", "DBT.FNS.1");
		}
		if (null == m_table) {
			throw new DScabiException("Table is null", "DBT.FNS.2");
		}
		log.debug("fieldNames() firstTime is {}", m_firstTime);
		log.debug("fieldNames() table.count() is {}", m_table.count());
		if (m_table.count() <= 0) {
			return null;
		}
		if (m_firstTime) {
			String map = "function() { for (var key in this) { emit(key, null); } }";
			String reduce = "function(key, s) { return null; }";
			
			//MapReduceCommand cmd = new MapReduceCommand(m_table, map, reduce,
			//		   	     null, MapReduceCommand.OutputType.INLINE, null);
			MapReduceIterable<Document> out = m_table.mapReduce(map, reduce);
			m_fieldNames = new LinkedList<String>();
			for (Document o : out) {
			    log.debug("fieldNames() Key, value is : {}", o.toString());
			    log.debug("fieldNames() Key name is : {}", o.get("_id").toString());
			    if (false == o.get("_id").toString().equals("_id"))
			    	m_fieldNames.add(o.get("_id").toString());
			}			
			m_firstTime = false;
			return m_fieldNames;
		}
		return m_fieldNames;
	}

	public LinkedList<String> fieldNamesUsingFindOne() throws DScabiException {
		if (null == m_tableName) {
			throw new DScabiException("Table name is null", "DBT.FNU.1");
		}
		if (null == m_table) {
			throw new DScabiException("Table is null", "DBT.FNU.2");
		}
		log.debug("fieldNamesUsingFindOne() firstTime is {}", m_firstTime);
		log.debug("fieldNamesUsingFindOne() table.count() is {}", m_table.count());
		if (m_table.count() <= 0) {
			return null;
		}
		if (m_firstTime) {
			FindIterable<Document> out = m_table.find();
			m_fieldNames = new LinkedList<String>();
			
			for (Document o : out) {
				Set<String> st = o.keySet();
				for (String s : st) {
				    log.debug("fieldNamesUsingFindOne() value is : {}", o.get(s));
				    log.debug("fieldNamesUsingFindOne() Key name is : {}", s);
				    if (false == s.equals("_id"))
				    	m_fieldNames.add(s);
				}	
				break;
			}
			m_firstTime = false;
			return m_fieldNames;
		}
		return m_fieldNames;
	}

	public MongoCollection<Document> getCollection() {
		return m_table;
	}

	public DResultSet find(DDocument document) {
		FindIterable<Document> cursor = m_table.find(document.getDocument());
		m_dcursor.set(cursor);
		return m_dcursor;
	}
	
	public DResultSet find(Bson document) {
		FindIterable<Document> cursor = m_table.find(document);
		m_dcursor.set(cursor);
		return m_dcursor;
	}
	
	public long count(DDocument document) {
		return m_table.count(document.getDocument());
	}

	public long count(Bson document) {
		return m_table.count(document);
	}

	public DResultSet findDirect(DDocument document) {
		return new DResultSet(m_table.find(document.getDocument()));
	}
	
	public DResultSet findDirect(Bson document) {
		return new DResultSet(m_table.find(document));
	}

	/*
	public DDocument findOne(DDocument document) {
		FindIterable<?> ob = m_table.findOne(document.getDocument());
		return new DDocument(ob);
	}
	*/
	
	public int insert(DDocument dob) throws DScabiException {
	
		m_table.insertOne(dob.getDocument());
		/*
		log.debug("insert() result is : {}", result.getN());
		if (result.getN() < 0)
			throw new DScabiException("Insert failed for DBackObject : " + dob.toString(), "DBT.INT.1");
		return result.getN();
		*/
		return 0;
	}

	public long update(DDocument dob, DDocument updateObj) throws DScabiException {
		UpdateResult result = m_table.updateMany(dob.getDocument(), updateObj.getDocument());
		log.debug("update() result is : {}", result.getModifiedCount());
		if (result.getModifiedCount() < 0)
			throw new DScabiException("Update failed for dob : " + dob.toString() + " updateObj : " + updateObj.toString(), "DBT.UPE.1");
		return result.getModifiedCount();
	}
	
	public long update(Bson dob, Bson updateObj) throws DScabiException {
		UpdateResult result = m_table.updateMany(dob, updateObj);
		log.debug("update() result is : {}", result.getModifiedCount());
		if (result.getModifiedCount() < 0)
			throw new DScabiException("Update failed for dob : " + dob.toString() + " updateObj : " + updateObj.toString(), "DBT.UPE.1");
		return result.getModifiedCount();
	}

	public long update(Bson dob, DDocument updateObj) throws DScabiException {
		UpdateResult result = m_table.updateMany(dob, updateObj.getDocument());
		log.debug("update() result is : {}", result.getModifiedCount());
		if (result.getModifiedCount() < 0)
			throw new DScabiException("Update failed for dob : " + dob.toString() + " updateObj : " + updateObj.toString(), "DBT.UPE.1");
		return result.getModifiedCount();
	}

	public long update(DDocument dob, Bson updateObj) throws DScabiException {
		UpdateResult result = m_table.updateMany(dob.getDocument(), updateObj);
		log.debug("update() result is : {}", result.getModifiedCount());
		if (result.getModifiedCount() < 0)
			throw new DScabiException("Update failed for dob : " + dob.toString() + " updateObj : " + updateObj.toString(), "DBT.UPE.1");
		return result.getModifiedCount();
	}

	public long remove(DDocument dob) throws DScabiException {
		DeleteResult result = m_table.deleteMany(dob.getDocument());
		log.debug("remove() result is : {}", result.getDeletedCount());
		if (result.getDeletedCount() < 0)
			throw new DScabiException("Remove failed for DBackObject : " + dob.toString(), "DBT.REE.1");
		return result.getDeletedCount();
	}
	
	public long remove(Bson dob) throws DScabiException {
		DeleteResult result = m_table.deleteMany(dob);
		log.debug("remove() result is : {}", result.getDeletedCount());
		if (result.getDeletedCount() < 0)
			throw new DScabiException("Remove failed for DBackObject : " + dob.toString(), "DBT.REE.1");
		return result.getDeletedCount();
	}

	public long removeAll() throws DScabiException {
		FindIterable<Document> cursor = m_table.find();
		MongoCursor<Document> cursorExist = cursor.iterator();
		while (cursorExist.hasNext()) {
			Document d = cursorExist.next();
			DeleteResult result = m_table.deleteMany(d);
			log.debug("remove() result is : {}", result.getDeletedCount());
			if (result.getDeletedCount() < 0)
				throw new DScabiException("Remove failed for DBackObject : " + d.toString(), "DBT.REE.1");
		}

		return 0;
	}

	
	private boolean isEmpty(LinkedList<String> fieldList) {
		
		if (null == fieldList)
			return true;
		if (fieldList.isEmpty())
			return true;
		return false;
	}

	public int insertRow(String jsonRow, String jsonCheck) throws DScabiException, IOException {

		log.debug("insertRow() firstTime is {}", m_firstTime);
		LinkedList<String> fieldList = fieldNamesUsingFindOne(); // fieldNames();
		DMJson djson = new DMJson(jsonRow);
		Set<String> st = djson.keySet();
		Document document = new Document();
		long n = 0;
		//WriteResult result = null;
		
		DMJson djsonCheck = new DMJson(jsonCheck);
		Set<String> stCheck = djsonCheck.keySet();
		Document documentCheck = new Document();
		
		if (false == isEmpty(fieldList)) {
			if (false == fieldList.containsAll(st)) {
		   		throw new DScabiException("One or more field name in jsonRow doesn't exist in fieldNames list. jsonRow : " + jsonRow + " Field Names list : " + fieldList, "DBT.IRW.1");
			}
			if (false == fieldList.containsAll(stCheck)) {
	    		throw new DScabiException("One or more field name in jsonCheck doesn't exist in fieldNames list. jsonCheck : " + jsonCheck + " Field Names list : " + fieldList, "DBT.IRW.2");				
	    	}
			if (false == st.containsAll(fieldList)) {
		   		throw new DScabiException("One or more field name in fieldNames doesn't exist in jsonRow key set. jsonRow : " + jsonRow + " Field Names list : " + fieldList, "DBT.IRW.3");
			}
			if (fieldList.size() != st.size()) {
	    		throw new DScabiException("Fields count doesn't match. fieldNames : " + fieldList.toString() + " with jsonRow : " + jsonRow, "DBT.IRW.4");
			}
		}
		
		if (false == isEmpty(fieldList)) {
			for (String fieldName : st) {
		    	// create a document to store key and value
				String f = djson.getString(fieldName);
				if (null == f) {
		    		throw new DScabiException("Field name " + fieldName + " doesn't exist in jsonRow : " + jsonRow + " Field Names list : " + fieldList, "DBT.IRW.5");
				}
		    	document.put(fieldName, f);
			}

			for (String keyCheck : stCheck) {
		    	// create a document to store key and value				
				String f2 = djsonCheck.getString(keyCheck);
				if (null == f2) {
		    		throw new DScabiException("Field name " + keyCheck + " doesn't exist in jsonCheck : " + jsonCheck, "DBT.IRW.6");
				}
		    	documentCheck.put(keyCheck, f2);
			}
			
	    	FindIterable<Document> cursorExist = m_table.find(documentCheck);
	    	n = m_table.count(documentCheck);
	    	if (0 == n) {
	    		log.debug("insertRow() Inside 0 == n");
	    		m_table.insertOne(document);
	    		/*
	    		log.debug("insertRow() result is : {}", result.getN());
	    		if (result.getN() < 0)
	    			throw new DScabiException("Insert failed for document : " + document.toString(), "DBT.IRW.7");
	    		*/
	    	} else if (1 == n) {
	    		throw new DScabiException("Row already exists. jsonCheck : " + jsonCheck, "DBT.IRW.8"); // already found
	    	} else {
	    		throw new DScabiException("Row already exists, multiple matches. jsonCheck : " + jsonCheck, "DBT.IRW.9"); // already found
	    	}
		} else {
			for (String key : st) {
		    	// create a document to store key and value
				String f3 = djson.getString(key);
				if (null == f3) {
		    		throw new DScabiException("Field name " + key + " doesn't exist in jsonRow : " + jsonRow, "DBT.IRW.10");
				}
				document.put(key, djson.getString(key));
			}
			
			for (String keyCheck : stCheck) {
		    	// create a document to store key and value				
				String f4 = djsonCheck.getString(keyCheck);
				if (null == f4) {
		    		throw new DScabiException("Field name " + keyCheck + " doesn't exist in jsonCheck : " + jsonCheck, "DBT.IRW.11");
				}
		    	documentCheck.put(keyCheck, djsonCheck.getString(keyCheck));
			}
			
	    	FindIterable<Document> cursorExist = m_table.find(documentCheck);
	    	n = m_table.count(documentCheck);
	    	if (0 == n) {
	    		log.debug("insertRow() Inside 0 == n");
		    	m_table.insertOne(document);
		    	/*
	    		log.debug("insertRow() result is : {}", result.getN());
	    		if (result.getN() < 0)
	    			throw new DScabiException("Insert failed for document : " + document.toString(), "DBT.IRW.12");
	    		*/
	    	} else if (1 == n) {
	    		throw new DScabiException("Row already exists. jsonCheck : " + jsonCheck, "DBT.IRW.13"); // already found
	    	} else {
	    		throw new DScabiException("Row already exists, multiple matches. jsonCheck : " + jsonCheck, "DBT.IRW.14"); // already found
	    	}
		}
		return 0;
	}

	public String executeQuery(String jsonQuery) throws DScabiException, IOException {
		LinkedList<String> fieldList = fieldNamesUsingFindOne(); // fieldNames();
		DMJson djson = new DMJson(jsonQuery);
		Set<String> st = djson.keySet();
		Document document = new Document();
		LinkedList<String> finalList = new LinkedList<String>();
		HashMap<String, String> hmap = new HashMap<String, String>();
		DMJson djson3 = null;
		
		if (false == isEmpty(fieldList)) {
			if (false == fieldList.containsAll(st)) {
		   		throw new DScabiException("One or more field name in jsonQuery doesn't exist in fieldNames list. jsonQuery : " + jsonQuery + " Field Names list : " + fieldList, "DBT.EQY.1");
			}
			
		}
	
		for (String key : st) {
		   	// create a document to store key and value
		   	document.put(key, djson.getString(key));
		}
	    FindIterable<Document> cursor = m_table.find(document);
	    MongoCursor<Document> cursorExist = cursor.iterator();
	    while (cursorExist.hasNext()) {
	    		
	    	hmap.clear();
	    	Document ob = cursorExist.next();
	    	Set<String> obkeys = ob.keySet();
			obkeys.remove("_id"); // exclude _id field
	    	//log.debug("executeQuery() result from ob {}", ob.toString());
			if (false == isEmpty(fieldList)) {
				if (false == obkeys.containsAll(fieldList)) {
			   		throw new DScabiException("One or more field name in fieldList doesn't exist in obkeys key set. obkeys : " + obkeys + " Field Names list : " + fieldList, "DBT.EQY.2");
				}
				for (String field : obkeys) {
					//if (field.equals("_id"))
					//	continue;
					String f = ob.getString(field);
					if (null == f) {
			    		throw new DScabiException("Field name " + field + " doesn't exist in dbobject in dbcursor. jsonQuery : " + jsonQuery + " Field Names list : " + fieldList, "DBT.EQY.3");
					}
					//log.debug("executeQuery() field is {}", field);
					//log.debug("executeQuery() f is {}", f);
					hmap.put(field, f);
				}
			} else {
		   		for (String key : obkeys) {
					//if (key.equals("_id"))
					//	continue;
		   			String f2 = ob.getString(key);
					if (null == f2) {
			    		throw new DScabiException("Field name " + key + " doesn't exist in dbobject in dbcursor. jsonQuery : " + jsonQuery, "DBT.EQY.4");
					}
					//log.debug("executeQuery() key is {}", key);
					//log.debug("executeQuery() f2 is {}", f2);
					hmap.put(key, f2);
				}
			}
			DMJson djson2 = null;
			//if (false == fieldList.isEmpty())
			//	djson2 = DJson.createDJsonList(hmap, fieldList);
			//else if (false == st.isEmpty())
			//	djson2 = DJson.createDJsonSet(hmap, st);
			if (false == obkeys.isEmpty())
				djson2 = DMJson.createDJsonSet(hmap, obkeys);
			if (null == djson2) {
	    		throw new DScabiException("djson2 is null. jsonQuery : " + jsonQuery, "DBT.EQY.5");
			}
			finalList.add(djson2.toString());
	    }
	    djson3 = DMJson.createDJsonWithCount(finalList);
		
		return djson3.toString();
	}

	public DResultSet executeQueryCursorResult(String jsonQuery) throws IOException, DScabiException {
		LinkedList<String> fieldList = fieldNamesUsingFindOne(); // fieldNames();
		DMJson djson = new DMJson(jsonQuery);
		Set<String> st = djson.keySet();
		Document document = new Document();

		if (false == isEmpty(fieldList)) {
			if (false == fieldList.containsAll(st)) {
		   		throw new DScabiException("One or more field name in jsonQuery doesn't exist in fieldNames list. jsonQuery : " + jsonQuery + " Field Names list : " + fieldList, "DBT.EQC.1");
			}
		}

		for (String key : st) {
		    	// create a document to store key and value
		    	document.put(key, djson.getString(key));
		}
	    FindIterable<Document> cursorExist = m_table.find(document);
	   	return new DResultSet(cursorExist);
	}

	public long executeUpdate(String jsonUpdate, String jsonWhere) throws IOException, DScabiException {
		LinkedList<String> fieldList = fieldNamesUsingFindOne(); // fieldNames();
		DMJson djsonWhere = new DMJson(jsonWhere);
		Set<String> stWhere = djsonWhere.keySet();
		Document documentWhere = new Document();
		
		DMJson djsonUpdate = new DMJson(jsonUpdate);
		Set<String> stUpdate = djsonUpdate.keySet();
		Document documentUpdate = new Document();

		if (false == isEmpty(fieldList)) {
			if (false == fieldList.containsAll(stWhere)) {
		   		throw new DScabiException("One or more field name in jsonWhere doesn't exist in fieldNames list. jsonWhere : " + jsonWhere + " Field Names list : " + fieldList, "DBT.EUE.1");
			}
			if (false == fieldList.containsAll(stUpdate)) {
	    		throw new DScabiException("One or more field name in jsonUpdate doesn't exist in fieldNames list. jsonUpdate : " + jsonUpdate + " Field Names list : " + fieldList, "DBT.EUE.2");				
	    	}
		}
		
		for (String keyWhere : stWhere) {
		    	// create a document to store key and value
		    	documentWhere.put(keyWhere, djsonWhere.getString(keyWhere));
		}

		for (String keyUpdate : stUpdate) {
	    	// create a document to store key and value
	    	documentUpdate.put(keyUpdate, djsonUpdate.getString(keyUpdate));
		}
	
	   	BasicDBObject updateObj = new BasicDBObject();
	   	updateObj.put("$set", documentUpdate);

	   	UpdateResult result = m_table.updateMany(documentWhere, updateObj);
		log.debug("executeUpdate() result is : {}", result.getModifiedCount());
	   	if (result.getModifiedCount() <= 0)
			throw new DScabiException("Update failed for documentWhere : " + documentWhere.toString() + " updateObj : " + updateObj.toString(), "DBT.EUE.3");
  	
		return result.getModifiedCount();
	
	}

	public long executeRemove(String jsonWhere) throws IOException, DScabiException {
		LinkedList<String> fieldList = fieldNamesUsingFindOne(); // fieldNames();
		DMJson djsonWhere = new DMJson(jsonWhere);
		Set<String> stWhere = djsonWhere.keySet();
		Document documentWhere = new Document();
		
		if (false == isEmpty(fieldList)) {
			if (false == fieldList.containsAll(stWhere)) {
		   		throw new DScabiException("One or more field name in jsonWhere doesn't exist in fieldNames list. jsonWhere : " + jsonWhere + " Field Names list : " + fieldList, "DBT.ERE.1");
			}
		}
		
		for (String keyWhere : stWhere) {
		    	// create a document to store key and value
		    	documentWhere.put(keyWhere, djsonWhere.getString(keyWhere));
		}

	   	// DBObject result = table.findAndRemove(documentWhere);
	   	DeleteResult result = m_table.deleteMany(documentWhere);
		log.debug("executeRemove() result is : {}", result.getDeletedCount());
	   	if (result.getDeletedCount() <= 0)
			throw new DScabiException("Remove failed for documentWhere : " + documentWhere.toString(), "DBT.ERE.2");

	   	return result.getDeletedCount();
	
	}

	
}
