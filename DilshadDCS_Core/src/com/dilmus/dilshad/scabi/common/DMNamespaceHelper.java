/**
 * @author Dilshad Mustafa
 * Copyright (c) Dilshad Mustafa
 * All Rights Reserved.
 * Created 03-Feb-2016
 * File Name : DMNamespaceHelper.java
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

package com.dilmus.dilshad.scabi.common;

import java.io.IOException;
import java.util.Date;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dilmus.dilshad.scabi.db.DDB;
import com.dilmus.dilshad.scabi.db.DDocument;
import com.dilmus.dilshad.scabi.db.DTable;

/**
 * @author Dilshad Mustafa
 *
 */
public class DMNamespaceHelper {

	final static Logger log = LoggerFactory.getLogger(DMNamespaceHelper.class);
	private DDB m_ddb = null;
	private DTable m_table = null;
	private DMDao m_ddao = null;
	
	private static final DMCounter M_DMCOUNTER = new DMCounter();
	
	public DMNamespaceHelper(DDB ddb) throws DScabiException { 
		m_ddb = ddb;
		if (false == ddb.tableExists("NamespaceTable")) {
			throw new DScabiException("Table name doesn't exist : NamespaceTable", "DNH.DNH.1");
		}
		m_table = ddb.getTable("NamespaceTable");
		m_ddao = new DMDao(ddb);
		m_ddao.setTableName("NamespaceTable");
	}
	
	public String register(DMJson dmjson) throws DScabiException, IOException {
		if ( false == dmjson.contains("Namespace")) {
			throw new DScabiException("Namespace is not found", "DNH.RER.1");
		}
		if ( false == dmjson.contains("Type")) {
			throw new DScabiException("Type is not found", "DNH.RER.2");
		}
		if ( false == dmjson.contains("Host")) {
			throw new DScabiException("Host is not found", "DNH.RER.3");
		}
		if ( false == dmjson.contains("Port")) {
			throw new DScabiException("Port is not found", "DNH.RER.4");
		}
		if ( false == dmjson.contains("UserID")) {
			throw new DScabiException("UserID is not found", "DNH.RER.5");
		}
		if ( false == dmjson.contains("Pwd")) {
			throw new DScabiException("Pwd is not found", "DNH.RER.6");
		}
		if ( false == dmjson.contains("SystemSpecificName")) {
			throw new DScabiException("SystemSpecificName is not found", "DNH.RER.7");
		}
		if ( false == dmjson.contains("SystemType")) {
			throw new DScabiException("SystemType is not found", "DNH.RER.8");
		}
			
		DTable t = m_ddb.getTable("NamespaceTable");

    	long n = 0;
    	DDocument document = new DDocument();
    	document.put("Namespace", dmjson.getString("Namespace"));
   
    	n = m_table.count(document);
    	String uuid1 = null;
    	if (0 == n) {
    		System.out.println("register() Inside 0 == n");
    		uuid1 = UUID.randomUUID().toString() + "_" + System.nanoTime() + "_" + M_DMCOUNTER.inc();
    		uuid1 = uuid1.replace('-', '_');
    		document.put("Type", dmjson.getString("Type"));
			document.put("Host", dmjson.getString("Host"));
 			document.put("Port", dmjson.getString("Port"));
 			document.put("UserID", dmjson.getString("UserID"));
			document.put("Pwd", dmjson.getString("Pwd"));
			document.put("SystemSpecificName", dmjson.getString("SystemSpecificName"));
			document.put("SystemType", dmjson.getString("SystemType"));
			document.put("SystemUUID", uuid1);
			document.put("RegisteredDate", (new Date()).toString());
			document.put("Status", "Available");
			document.put("StatusDate", (new Date()).toString());
  	    	
	    	m_table.insert(document);
	    	return uuid1;
    	}
    	else
    		throw new DScabiException("Namespace already exists : " + dmjson.getString("Namespace"), "DNH.RER.9"); // already found or multiple matches

	}
	
	public boolean namespaceExists(String strNamespace) throws IOException, DScabiException {
		if ( null == strNamespace) {
			throw new DScabiException("strNamespace is null", "DNH.RER.10");
		}
		String jsonQuery = "{ \"Namespace\" : \"" + strNamespace + "\" }";
		String jsonResult = m_ddao.executeQuery(jsonQuery);
		
		DMJson djson = new DMJson(jsonResult);
		String count = djson.getString("Count");
		int n = Integer.parseInt(count);
		
		if (0 == n) {
    		//throw new DScabiException("No entry found for namespace : " + strNamespace, "DNH.GMT.1");
    		return false;
		}
		else if (n > 1) {
    		//throw new DScabiException("Multiple entries found for namespace : " + strNamespace, "DNH.GMT.2");
    		return true;
		}

		return true;		
	}
	
	public DMNamespace getNamespace(String strNamespace) throws IOException, DScabiException {
		String jsonQuery = "{ \"Namespace\" : \"" + strNamespace + "\" }";
		String jsonResult = m_ddao.executeQuery(jsonQuery);
		
		DMJson djson = new DMJson(jsonResult);
		String count = djson.getString("Count");
		int n = Integer.parseInt(count);
		
		if (0 == n) {
    		throw new DScabiException("No entry found for namespace : " + strNamespace, "DNH.GMT.1");
		}
		else if (n > 1) {
    		throw new DScabiException("Multiple entries found for namespace : " + strNamespace, "DNH.GMT.2");
		}

		String jsonString = djson.getString("1");
		DMJson djson2 = new DMJson(jsonString);
		
		return new DMNamespace(djson2);

		/*
		switch(metaType) {
			case "MetaThis" : return getMetaThis(nameSpace);
			case "MetaRemote" : return getMetaRemote(nameSpace);
			case "AppTable" : return getAppTable(nameSpace);
			case "JavaFile" : return getJavaFile(nameSpace);
			case "File" : return getFile(nameSpace);
			default : throw new DScabiException("Invalid meta type : " + metaType, "DNH.GNE.1");
		}
		*/
		
	}
	
	public DMNamespace getNamespaceByJsonStrQuery(String strNamespace, String strType) throws IOException, DScabiException {
		String jsonStrQuery = "{ \"Namespace\" : \"" + strNamespace + "\", \"Type\" : \"" + strType + "\" }";
		//String jsonStrQuery = "{ \"Type\" : \"" +  strType + "\", \"Namespace\" : \"" + strNamespace + "\" }";

		String jsonResult = m_ddao.executeQuery(jsonStrQuery);
		
		DMJson djson = new DMJson(jsonResult);
		String count = djson.getString("Count");
		int n = Integer.parseInt(count);
		
		if (0 == n) {
    		throw new DScabiException("No entry found for namespace query : " + jsonStrQuery, "DNH.GMT.1");
		}
		else if (n > 1) {
    		throw new DScabiException("Multiple entries found for namespace query : " + jsonStrQuery, "DNH.GMT.2");
		}

		String jsonString = djson.getString("1");
		DMJson djson2 = new DMJson(jsonString);
		
		return new DMNamespace(djson2);
		
	}

	public DMNamespace findOneNamespace(String metaType) throws IOException, DScabiException {
		
		String type = metaType;
		String jsonQuery = "{ \"Type\" : \"" + type + "\" }";
		String jsonResult = m_ddao.executeQuery(jsonQuery);
		
		DMJson djson = new DMJson(jsonResult);
		String count = djson.getString("Count");
		int n = Integer.parseInt(count);
		
		if (0 == n) {
    		throw new DScabiException("No entry for type " + type + " found for any namespace", "DNH.GFN.1");
		}

		String jsonString = djson.getString("1");
		DMJson djson2 = new DMJson(jsonString);
		
		return new DMNamespace(djson2);
	}
	
	/*
	public DNamespace getMetaThis(String nameSpace) throws IOException, DScabiException {
		
		String type = "MetaThis";
		String jsonQuery = "{ \"Type\" : \"" + type + "\", \"Namespace\" : \"" + nameSpace + "\" }";
		String jsonResult = ddao.executeQuery(jsonQuery);
		
		DJson djson = new DJson(jsonResult);
		String count = djson.getString("Count");
		int n = Integer.parseInt(count);
		
		if (0 == n) {
    		throw new DScabiException("No type " + type + " found for namespace : " + nameSpace, "DNH.GMT.1");
		}
		else if (n > 1) {
    		throw new DScabiException("Multiple type " + type + " found for namespace : " + nameSpace, "DNH.GMT.2");
		}

		String jsonString = djson.getString("1");
		DJson djson2 = new DJson(jsonString);
		
		return new DNamespace(djson2);
	}
	*/
	
	/*
	public DNamespace getMetaRemote(String nameSpace) throws IOException, DScabiException {
		
		String type = "MetaRemote";
		String jsonQuery = "{ \"Type\" : \"" + type + "\", \"Namespace\" : \"" + nameSpace + "\" }";
		String jsonResult = ddao.executeQuery(jsonQuery);
		
		DJson djson = new DJson(jsonResult);
		String count = djson.getString("Count");
		int n = Integer.parseInt(count);
		
		if (0 == n) {
    		throw new DScabiException("No type " + type + " found for namespace : " + nameSpace, "DNH.GMR.1");
		}
		else if (n > 1) {
    		throw new DScabiException("Multiple type " + type + " found for namespace : " + nameSpace, "DNH.GMR.2");
		}

		String jsonString = djson.getString("1");
		DJson djson2 = new DJson(jsonString);
		
		return new DNamespace(djson2);
	}
	*/
	
	/*
	public DNamespace getAppTable(String nameSpace) throws IOException, DScabiException {
		
		String type = "AppTable";
		String jsonQuery = "{ \"Type\" : \"" + type + "\", \"Namespace\" : \"" + nameSpace + "\" }";
		String jsonResult = ddao.executeQuery(jsonQuery);
		
		DJson djson = new DJson(jsonResult);
		String count = djson.getString("Count");
		int n = Integer.parseInt(count);
		
		if (0 == n) {
    		throw new DScabiException("No type " + type + " found for namespace : " + nameSpace, "DNH.GAT.1");
		}
		else if (n > 1) {
    		throw new DScabiException("Multiple type " + type + " found for namespace : " + nameSpace, "DNH.GAT.2");
		}

		String jsonString = djson.getString("1");
		DJson djson2 = new DJson(jsonString);
		
		return new DNamespace(djson2);
	}
	*/
	
	/*
	public DNamespace getJavaFile(String nameSpace) throws IOException, DScabiException {
		
		String type = "JavaFile";
		String jsonQuery = "{ \"Type\" : \"" + type + "\", \"Namespace\" : \"" + nameSpace + "\" }";
		String jsonResult = ddao.executeQuery(jsonQuery);
		
		DJson djson = new DJson(jsonResult);
		String count = djson.getString("Count");
		int n = Integer.parseInt(count);
		
		if (0 == n) {
    		throw new DScabiException("No type " + type + " found for namespace : " + nameSpace, "DNH.GJF.1");
		}
		else if (n > 1) {
    		throw new DScabiException("Multiple type " + type + " found for namespace : " + nameSpace, "DNH.GJF.2");
		}

		String jsonString = djson.getString("1");
		DJson djson2 = new DJson(jsonString);
		
		return new DNamespace(djson2);
	}
	*/
	
	/*
	public DNamespace getFile(String nameSpace) throws IOException, DScabiException {
		
		String type = "File";
		String jsonQuery = "{ \"Type\" : \"" + type + "\", \"Namespace\" : \"" + nameSpace + "\" }";
		String jsonResult = ddao.executeQuery(jsonQuery);
		
		DJson djson = new DJson(jsonResult);
		String count = djson.getString("Count");
		int n = Integer.parseInt(count);
		
		if (0 == n) {
    		throw new DScabiException("No type " + type + " found for namespace : " + nameSpace, "DNH.GFE.1");
		}
		else if (n > 1) {
    		throw new DScabiException("Multiple type " + type + " found for namespace : " + nameSpace, "DNH.GFE.2");
		}

		String jsonString = djson.getString("1");
		DJson djson2 = new DJson(jsonString);
		
		return new DNamespace(djson2);
	}
	*/
}

