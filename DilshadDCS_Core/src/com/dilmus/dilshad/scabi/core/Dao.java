/**
 * @author Dilshad Mustafa
 * Copyright (c) Dilshad Mustafa
 * All Rights Reserved.
 * @since 09-Mar-2016
 * File Name : Dao.java
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

7. Under differently named or renamed software, you should not redistribute this 
Software and/or any modified works of this Software, including its source code 
and/or its compiled object binary form. Under your name or your company name or 
your product name, you should not publish this Software, including its source code 
and/or its compiled object binary form, modified or original. 

8. You agree to use the original source code from Dilshad Mustafa's project only
and/or the compiled object binary form of the original source code.

9. You agree fully to the terms and conditions of this License of this software product, 
under same software name and/or if it is renamed in future.

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

package com.dilmus.dilshad.scabi.core;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dilmus.dilshad.scabi.common.DMUtil;
import com.dilmus.dilshad.scabi.common.DScabiException;
import com.dilmus.dilshad.scabi.db.DDB;
import com.dilmus.dilshad.scabi.db.DTable;
import com.dilmus.dilshad.scabi.deprecated.DBackFileOld;
import com.dilmus.dilshad.scabi.deprecated.DDBOld;
import com.dilmus.dilshad.scabi.deprecated.DTableOld;
import com.dilmus.dilshad.scabi.deprecated.Dao2;

/**
 * @author Dilshad Mustafa
 *
 */
public class Dao {



	final static Logger log = LoggerFactory.getLogger(Dao.class);
	//private String m_tableName = null;
	private DDB m_ddb = null;
	//private DTable m_table = null;
	private DNamespace m_namespace = null;
	private String m_strNamespace = null;
	private DMeta m_meta = null;
	private HashMap<String, DDB> m_mapNamespaceStrDDB = null;
	
	public Dao(DMeta meta) throws IOException, ParseException, DScabiClientException, DScabiException {
		m_meta = meta;
		
		m_mapNamespaceStrDDB = new HashMap<String, DDB>();
		//m_tableName = null;
		m_ddb = null;
		//m_table = null;
		m_namespace = null;
		m_strNamespace = null;
	}
	
	public int close() {
		Set<String> st = m_mapNamespaceStrDDB.keySet();
		for (String s : st) {
			DDB ddb = m_mapNamespaceStrDDB.get(s);
			ddb.close();
		}
		return 0;
		
	}
	
	public int setNamespace(String strNamespace) throws DScabiException, IOException, DScabiClientException {
		
		m_namespace = null;
		m_strNamespace = null;
		
		// // if (null != m_ddb)
		// // m_ddb.close();
		if (null == strNamespace) {
			throw new DScabiClientException("strNamespace is null", "DFE.PUT.1");
		}
		DNamespace namespace = m_meta.getNamespace(strNamespace, DNamespace.APPTABLE);
		m_ddb = new DDB(namespace.getHost(), namespace.getPort(), namespace.getSystemSpecificName());

		m_namespace = namespace;
		m_strNamespace = namespace.getName();
		m_mapNamespaceStrDDB.put(m_strNamespace, m_ddb);
		
		return 0;
		
	}
	
	public int setNamespace(DNamespace namespace) throws DScabiException, IOException, DScabiClientException {
		
		m_namespace = null;
		m_strNamespace = null;
		
		// // if (null != m_ddb)
		// // 	m_ddb.close();
		if (null == namespace) {
			throw new DScabiClientException("namespace is null", "DFE.SNE2.1");
		}
		if (false == namespace.getType().equals(DNamespace.APPTABLE)) {
			throw new DScabiClientException("Namespace type is not AppTable type. Actual type : " + namespace.getType(), "DFE.SNE2.2");
		}
		m_ddb = new DDB(namespace.getHost(), namespace.getPort(), namespace.getSystemSpecificName());

		m_namespace = namespace;
		m_strNamespace = namespace.getName();
		m_mapNamespaceStrDDB.put(m_strNamespace, m_ddb);

		return 0;
		
	}


	public DTable getTable(String tableName) throws DScabiException, org.apache.http.ParseException, IOException, DScabiClientException {
		if (null == tableName) {
			throw new DScabiClientException("tableName is null", "DFE.SNE2.1");
		}
		if (DMUtil.isNamespaceURLStr(tableName)) {
			String strNamespace = DMUtil.getNamespaceStr(tableName);
			log.debug("strNamespace : {}", strNamespace);
			String resourceName = DMUtil.getResourceName(tableName);
			log.debug("resourceName : {}", resourceName);
			DDB ddb = null;
			
			if (m_mapNamespaceStrDDB.containsKey(strNamespace)) {
				ddb = m_mapNamespaceStrDDB.get(strNamespace);
				return ddb.getTable(resourceName);
				
			} else {
				DNamespace namespace = m_meta.getNamespace(strNamespace, DNamespace.APPTABLE);
				log.debug("namespace : {}", namespace.toString());
				ddb = new DDB(namespace.getHost(), namespace.getPort(), namespace.getSystemSpecificName());
				m_mapNamespaceStrDDB.put(strNamespace, ddb);
				return ddb.getTable(resourceName);
			}
		
		} else {
			if (null == m_namespace) {
				throw new DScabiClientException("Namespace is not set", "DFE.PUT2.1");
			}
			return m_ddb.getTable(tableName);
		
		}
	
	}

	public boolean tableExists(String tableName) throws DScabiException, org.apache.http.ParseException, IOException, DScabiClientException {
		if (null == tableName) {
			throw new DScabiClientException("tableName is null", "DFE.SNE2.1");
		}
		if (DMUtil.isNamespaceURLStr(tableName)) {
			String strNamespace = DMUtil.getNamespaceStr(tableName);
			log.debug("strNamespace : {}", strNamespace);
			String resourceName = DMUtil.getResourceName(tableName);
			log.debug("resourceName : {}", resourceName);
			DDB ddb = null;
			
			if (m_mapNamespaceStrDDB.containsKey(strNamespace)) {
				ddb = m_mapNamespaceStrDDB.get(strNamespace);
				return ddb.tableExists(resourceName);
				
			} else {
				DNamespace namespace = m_meta.getNamespace(strNamespace, DNamespace.APPTABLE);
				log.debug("namespace : {}", namespace.toString());
				ddb = new DDB(namespace.getHost(), namespace.getPort(), namespace.getSystemSpecificName());
				m_mapNamespaceStrDDB.put(strNamespace, ddb);
				return ddb.tableExists(resourceName);
			}
		
		} else {
			if (null == m_namespace) {
				throw new DScabiClientException("Namespace is not set", "DFE.PUT2.1");
			}
			return m_ddb.tableExists(tableName);
		
		}
	
	}

	public DTable createTable(String tableName) throws DScabiException, org.apache.http.ParseException, IOException, DScabiClientException {
		if (null == tableName) {
			throw new DScabiClientException("tableName is null", "DFE.SNE2.1");
		}
		if (DMUtil.isNamespaceURLStr(tableName)) {
			String strNamespace = DMUtil.getNamespaceStr(tableName);
			log.debug("strNamespace : {}", strNamespace);
			String resourceName = DMUtil.getResourceName(tableName);
			log.debug("resourceName : {}", resourceName);
			DDB ddb = null;
			
			if (m_mapNamespaceStrDDB.containsKey(strNamespace)) {
				ddb = m_mapNamespaceStrDDB.get(strNamespace);
				return ddb.createTable(resourceName);
				
			} else {
				DNamespace namespace = m_meta.getNamespace(strNamespace, DNamespace.APPTABLE);
				log.debug("namespace : {}", namespace.toString());
				ddb = new DDB(namespace.getHost(), namespace.getPort(), namespace.getSystemSpecificName());
				m_mapNamespaceStrDDB.put(strNamespace, ddb);
				return ddb.createTable(resourceName);
			}
		
		} else {
			if (null == m_namespace) {
				throw new DScabiClientException("Namespace is not set", "DFE.PUT2.1");
			}
			return m_ddb.createTable(tableName);
		
		}
	
	}
}
