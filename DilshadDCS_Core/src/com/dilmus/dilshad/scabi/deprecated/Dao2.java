/**
 * @author Dilshad Mustafa
 * Copyright (c) Dilshad Mustafa
 * All Rights Reserved.
 * @since 28-Jan-2016
 * File Name : Dao2.java
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
to another programming language and embedded modified versions of this Software source 
code and/or its compiled object binary in any form, both within as well as outside your 
organization, company, legal entity and/or individual. 

6. You should not embed any modification of this Software source code and/or its compiled 
object binary form in any way, either partially or fully.

7. You should not redistribute this Software, including its source code and/or its 
compiled object binary form, under differently named or renamed software. You should 
not publish this Software, including its source code and/or its compiled object binary 
form, modified or original, under your name or your company name or your product name. 
You should not sell this Software to any party, organization, company, legal entity 
and/or individual.

8. You agree fully to the terms and conditions of this License of this software product, 
under same software name and/or if it is renamed in future.

9. This software is created and programmed by Dilshad Mustafa and Dilshad holds the 
copyright for this Software and all its source code. You agree that you will not infringe 
or do any activity that will violate Dilshad's copyright of this software and all its 
source code.

10. THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
   EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
   MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
   NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHOR OR COPYRIGHT HOLDER
   BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
   ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
   CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
   SOFTWARE.

*/

package com.dilmus.dilshad.scabi.deprecated;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.http.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dilmus.dilshad.scabi.common.DMJson;
import com.dilmus.dilshad.scabi.common.DScabiException;
import com.dilmus.dilshad.scabi.core.DMeta;
import com.dilmus.dilshad.scabi.core.DNamespace;
import com.dilmus.dilshad.scabi.core.DScabiClientException;
import com.dilmus.dilshad.scabi.db.DDB;
import com.dilmus.dilshad.scabi.db.DResultSet;
import com.dilmus.dilshad.scabi.db.DTable;

/**
 * @author Dilshad Mustafa
 *
 */
public class Dao2 {

	final static Logger log = LoggerFactory.getLogger(Dao2.class);
	private String m_tableName = null;
	private boolean m_firstTime = true;
	private ArrayList<String> m_fieldNames = null;
	
	private DDB m_ddb = null;
	private DTable m_table = null;
	private DNamespace m_namespace = null;
	
	public Dao2(DDB ddb) {
		m_ddb = ddb;
		m_tableName = null;
		m_firstTime = true;
		m_fieldNames = null;
		m_table = null;
		m_namespace = null;
	}
	
	public Dao2(String dbHost, String dbPort, String dbName) {
		m_ddb = new DDB(dbHost, dbPort, dbName);
		
		m_tableName = null;
		m_firstTime = true;
		m_fieldNames = null;
		m_table = null;
		m_namespace = null;
		
	}
	
	public Dao2(DNamespace namespace) throws DScabiException {
		if (false == namespace.getType().equals("AppTable")) {
			throw new DScabiException("Namespace type is not AppTable", "DAO.SDE.1");
		}
		m_ddb = new DDB(namespace.getHost(), namespace.getPort(), namespace.getSystemSpecificName());
		
		m_tableName = null;
		m_firstTime = true;
		m_fieldNames = null;
		m_table = null;
		m_namespace = namespace;
		
	}

	public Dao2(DMeta meta, String strNamespace) throws IOException, ParseException, DScabiClientException, DScabiException {
		
		DNamespace namespace = meta.getNamespace(strNamespace);
		if (false == namespace.getType().equals("AppTable")) {
			throw new DScabiException("Namespace type is not AppTable", "DAO.SDE.1");
		}
		m_ddb = new DDB(namespace.getHost(), namespace.getPort(), namespace.getSystemSpecificName());
		
		m_tableName = null;
		m_firstTime = true;
		m_fieldNames = null;
		m_table = null;
		m_namespace = namespace;

	}
	
	public int close() {
		if (m_ddb != null)
			m_ddb.close();
		m_ddb = null;
		return 0;
		
	}

	public int setDatabase(String dbHost, String dbPort, String dbName) {
		close();
		m_tableName = null;
		m_firstTime = true;
		m_fieldNames = null;
		m_table = null;
		m_namespace = null;
		m_ddb = null;
		
		m_ddb = new DDB(dbHost, dbPort, dbName);
		return 0;
	}

	public int setNamespace(DNamespace namespace) throws DScabiException {
		if (false == namespace.getType().equals("AppTable")) {
			close();
			
			m_tableName = null;
			m_firstTime = true;
			m_fieldNames = null;
			m_table = null;
			m_namespace = null;
			m_ddb = null;

			throw new DScabiException("Namespace type is not AppTable", "DAO.SDE.1");
		}
		close();
		
		m_tableName = null;
		m_firstTime = true;
		m_fieldNames = null;
		m_table = null;
		m_namespace = null;
		m_ddb = null;

		m_ddb = new DDB(namespace.getHost(), namespace.getPort(), namespace.getSystemSpecificName());
		m_namespace = namespace;
		return 0;
	}
	
	public int setNamespace(String strNamespace) throws DScabiException, IOException {
		DNamespace namespace = new DNamespace(strNamespace);
		if (false == namespace.getType().equals("AppTable")) {
			close();
			
			m_tableName = null;
			m_firstTime = true;
			m_fieldNames = null;
			m_table = null;
			m_namespace = null;
			m_ddb = null;

			throw new DScabiException("Namespace type is not AppTable", "DAO.SDE.1");
		}
		close();
		
		m_tableName = null;
		m_firstTime = true;
		m_fieldNames = null;
		m_table = null;
		m_namespace = null;
		m_ddb = null;

		m_ddb = new DDB(namespace.getHost(), namespace.getPort(), namespace.getSystemSpecificName());
		m_namespace = namespace;
		return 0;
	}

	public int setTableName(String tableName) throws DScabiException {
		m_tableName = null;
		m_firstTime = true;
		m_fieldNames = null;
		m_table = null;
		if (false == m_ddb.tableExists(tableName)) {
			throw new DScabiException("Table name doesn't exist : " + tableName, "DDO.STN.1");
		}
		m_table = m_ddb.getTable(tableName);
		m_tableName = tableName;
		return 0;
	}

	public ArrayList<String> fieldNames() throws DScabiException {
		if (null == m_tableName) {
			throw new DScabiException("Table name is null", "DDO.FNS.1");
		}
		if (null == m_table) {
			throw new DScabiException("Table is null", "DDO.FNS.2");
		}
		log.debug("fieldNames() firstTime is {}", m_firstTime);
		if (m_table.count() <= 0) {
			log.debug("fieldNames() table.count() is {}", m_table.count());
			return null;
		}
		if (m_firstTime) {
			m_fieldNames = m_table.fieldNamesUsingFindOne();
			m_firstTime = false;
			return m_fieldNames;

		}
		return m_fieldNames;
	}

	public DTable getTable() {
		return m_table;
	}

	public boolean isEmpty(ArrayList<String> fieldList) {
		if (null == fieldList)
			return true;
		if (fieldList.isEmpty())
			return true;
		return false;
	}
	
	public int insertRow(String jsonRow, String jsonCheck) throws DScabiException, IOException {
		return m_table.insertRow(jsonRow, jsonCheck);
	}
	
	public String executeQuery(String jsonQuery) throws DScabiException, IOException {
		return m_table.executeQuery(jsonQuery);
	}
	
	public DResultSet executeQueryCursorResult(String jsonQuery) throws IOException, DScabiException {
		return m_table.executeQueryCursorResult(jsonQuery);
	}
	
	public long executeUpdate(String jsonUpdate, String jsonWhere) throws IOException, DScabiException {
		return m_table.executeUpdate(jsonUpdate, jsonWhere);
	}
	
	public long executeRemove(String jsonWhere) throws IOException, DScabiException {
		return m_table.executeRemove(jsonWhere);
	}
	
	public int getResultCount(String jsonResult) throws IOException {
		DMJson djson = new DMJson(jsonResult);
		String s = djson.getString("Count");
		return Integer.parseInt(s);
	}
	
}
