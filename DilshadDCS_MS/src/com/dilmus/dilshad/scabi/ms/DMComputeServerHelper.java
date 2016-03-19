/**
 * @author Dilshad Mustafa
 * Copyright (c) Dilshad Mustafa
 * All Rights Reserved.
 * @since 26-Jan-2016
 * File Name : DMComputeServerHelper.java
 */

/**
Copyright (c) Dilshad Mustafa 2016. All Rights Reserved.

User and Developer License

1. You can use this Software for both Personal use as well as Commercial use, 
with or without paying fee to Dilshad Mustafa. Please read fully below for the 
terms and conditions. You may use this Software or any of its file contents, 
either partially or fully, only if you comply fully with all the terms and 
conditions of this License. 

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
organization, company, legal entity and/or individual. You should not embed any 
modification of this Software source code and/or its compiled object binary in any form, 
either partially or fully.

6. You should not redistribute this Software, including its source code and/or its 
compiled object binary form, under differently named or renamed software. You should 
not publish this Software, including its source code and/or its compiled object binary 
form, modified or original, under your name or your company name or your product name. 
You should not sell this Software to any party, organization, company, legal entity 
and/or individual.

7. You agree fully to the terms and conditions of this License of this software product, 
under same software name and/or if it is renamed in future.

8. This software is created and programmed by Dilshad Mustafa and Dilshad holds the 
copyright for this Software and all its source code. You agree that you will not infringe 
or do any activity that will violate Dilshad's copyright of this software and all its 
source code.

9. THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
   EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
   MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
   NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHOR OR COPYRIGHT HOLDER
   BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
   ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
   CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
   SOFTWARE.

*/

package com.dilmus.dilshad.scabi.ms;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Set;

import org.bson.BasicBSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dilmus.dilshad.scabi.common.DMJson;
import com.dilmus.dilshad.scabi.common.DMUtil;
import com.dilmus.dilshad.scabi.common.DScabiException;
import com.dilmus.dilshad.scabi.db.DDB;
import com.dilmus.dilshad.scabi.db.DDocument;
import com.dilmus.dilshad.scabi.db.DResultSet;
import com.dilmus.dilshad.scabi.db.DTable;
import com.dilmus.dilshad.scabi.deprecated.DDBOld;
import com.dilmus.dilshad.scabi.deprecated.DObject;
import com.dilmus.dilshad.scabi.deprecated.DResultSetOld;
import com.dilmus.dilshad.scabi.deprecated.DTableOld;
import com.mongodb.DBObject;

/**
 * @author Dilshad Mustafa
 *
 */
public class DMComputeServerHelper {

	private final Logger log = LoggerFactory.getLogger(DMComputeServerHelper.class);
	private DDB m_ddb = null;
	private DTable m_table = null;

	public DMComputeServerHelper(DDB ddb) throws DScabiException {
		m_ddb = ddb;
		m_table = ddb.getTable("ComputeMetaDataTable");
	}

	public DMComputeServer register(String fullHostName, String port, String maxCSThreads) throws DScabiException {
    	long n = 0;
		// Insert
    	DDocument document = new DDocument();
    	document.put("ComputeHost", fullHostName);
    	document.put("ComputePort", port);
    	
    	//DResultSet cursorExist = m_table.find(document);
    	n = m_table.count(document);
    	if (0 == n) {
    		log.debug("Inside 0 == n");
    		document.put("MAXCSTHREADS", maxCSThreads);
	    	document.put("RegisteredDate", (new Date()).toString());
        	document.put("Status", "Available"); // Available, Inuse, Hold, Blocked
	    	document.put("StatusDate", (new Date()).toString());

	    	m_table.insert(document);
    	}
    	else
    		throw new DScabiException("ComputeHost, ComputePort already exists", "CMH.RER.1"); // already found or multiple matches
    	return new DMComputeServer(m_ddb, fullHostName, port, maxCSThreads);
	}

	public DMComputeServer register(String jsonString) throws DScabiException, IOException {
		DMJson djson = new DMJson(jsonString);
		return register(djson.getString("ComputeHost"), djson.getString("ComputePort"), djson.getString("MAXCSTHREADS"));
	}
	
	public DMComputeServer alloc() throws DScabiException {

    	DDocument document = new DDocument();
    	document.put("Status", "Available");
    	
    	DResultSet result = m_table.find(document);
    	DDocument d = null;
    	if (null == result)
    		throw new DScabiException("No ComputeHost with status as Available", "CMH.ALC.1");
    	while (result.hasNext()) {
       		d = result.next();
    		break;
    	}
    	if (null == d)
    		throw new DScabiException("No ComputeHost with status as Available", "CMH.ALC.2");
    	log.debug("alloc() ComputeHost Alloc : {}", d.getString("ComputeHost"));
    	log.debug("alloc() ComputePort Alloc : {}", d.getString("ComputePort"));
    	log.debug("alloc() ComputePort Alloc : {}", d.getString("MAXCSTHREADS"));
    	 
    	return new DMComputeServer(m_ddb, d.getString("ComputeHost"), d.getString("ComputePort"), d.getString("MAXCSTHREADS"));
	}
	
	
	public List<DMComputeServer> getMany(int howMany) throws DScabiException {
    	
    	DDocument document = new DDocument();
    	document.put("Status", "Available");
    	
    	DResultSet cursorExist = m_table.find(document);
    	if (0 == m_table.count(document))
    		throw new DScabiException("Zero ComputeHost with status as Available", "CMH.GMY.2");
    	if (null == cursorExist)
    		throw new DScabiException("result set is null. No ComputeHost with status as Available", "CMH.GMY.1");
    	int k = 0;
    	//DMComputeServer cma[] = new DMComputeServer[howMany];
    	List<DMComputeServer> cma = new ArrayList<DMComputeServer>();
	    while (cursorExist.hasNext()) {
	    	if (k >= howMany) {
	    		break;
	    	}
	    	DDocument ob = cursorExist.next();
	    	log.debug("getMany() result from ob {}", ob.toString());
			String f = ob.getString("ComputeHost");
			if (null == f) {
	    		throw new DScabiException("Field name " + "ComputeHost" + " doesn't exist in dbobject in dbcursor.", "CMH.GMY.3");
			}
	    	log.debug("getMany() ComputeHost : {}", f);
			String f2 = ob.getString("ComputePort");
			if (null == f2) {
	    		throw new DScabiException("Field name " + "ComputePort" + " doesn't exist in dbobject in dbcursor.", "CMH.GMY.4");
			}
	    	log.debug("getMany() ComputePort : {}", f2);
	    	
			String f3 = ob.getString("MAXCSTHREADS");
			if (null == f3) {
	    		throw new DScabiException("Field name " + "MAXCSTHREADS" + " doesn't exist in dbobject in dbcursor.", "CMH.GMY.5");
			}
	    	log.debug("getMany() MAXCSTHREADS : {}", f3);

	    	//cma[k] = new DMComputeServer(m_ddb, f, f2);
	    	cma.add(new DMComputeServer(m_ddb, f, f2, f3));
	    	k++;
	    }
	    /*
	    DMComputeServer cma2[] = new DMComputeServer[k];
    	for (int i = 0; i < k; i++) {
    		cma2[i] = cma[i];
    		log.debug("getMany() cma i={} : {}", cma[i]); 
    		log.debug("getMany() cma2 i={} : {}", cma2[i]); 
    		
    		
    	}
	    return cma2; 
	    */
	    return cma;
	}

	public List<DMComputeServer> getManyMayExclude(int howMany, String jsonStrExclude) throws DScabiException, IOException {
		
    	DDocument document = new DDocument();
    	document.put("Status", "Available");
    	
    	DResultSet cursorExist = m_table.find(document);
    	if (0 == m_table.count(document))
    		throw new DScabiException("Zero ComputeHost with status as Available", "CMH.GMY.2");
    	if (null == cursorExist)
    		throw new DScabiException("result set is null. No ComputeHost with status as Available", "CMH.GMY.1");
    	int k = 0;
    	long n = m_table.count(document);
    	//DMComputeServer cma[] = new DMComputeServer[(int)n];
    	List<DMComputeServer> cma = new ArrayList<DMComputeServer>();
	    while (cursorExist.hasNext()) {
	    	DDocument ob = cursorExist.next();
	    	log.debug("getMany() result from ob {}", ob.toString());
			String f = ob.getString("ComputeHost");
			if (null == f) {
	    		throw new DScabiException("Field name " + "ComputeHost" + " doesn't exist in dbobject in dbcursor.", "CMH.GMY.3");
			}
	    	log.debug("getMany() ComputeHost : {}", f);
			String f2 = ob.getString("ComputePort");
			if (null == f2) {
	    		throw new DScabiException("Field name " + "ComputePort" + " doesn't exist in dbobject in dbcursor.", "CMH.GMY.4");
			}
	    	log.debug("getMany() ComputePort : {}", f2);
			String f3 = ob.getString("MAXCSTHREADS");
			if (null == f3) {
	    		throw new DScabiException("Field name " + "MAXCSTHREADS" + " doesn't exist in dbobject in dbcursor.", "CMH.GMY.5");
			}
	    	log.debug("getMany() MAXCSTHREADS : {}", f3);

	    	//cma[k] = new DMComputeServer(m_ddb, f, f2);
	    	cma.add(new DMComputeServer(m_ddb, f, f2, f3));
	    	k++;
	    }
	    exclude(cma, jsonStrExclude);
	    while (cma.size() > howMany) {
	    	cma.remove(cma.size() - 1);
	    }
	    /*
	    DMComputeServer cma2[] = new DMComputeServer[k];
    	for (int i = 0; i < k; i++) {
    		cma2[i] = cma[i];
    		log.debug("getMany() cma i={} : {}", cma[i]); 
    		log.debug("getMany() cma2 i={} : {}", cma2[i]); 
    		
    		
    	}
	    return cma2; 
	    */
	    return cma;
	}
	
	public List<DMComputeServer> getAllAvailable() throws DScabiException {
		
    	// create a document to store key and value
    	DDocument document = new DDocument();
    	document.put("Status", "Available");
    	
    	DResultSet cursorExist = m_table.find(document);
    	if (0 == m_table.count(document))
    		throw new DScabiException("Zero ComputeHost with status as Available", "CMH.GMY.2");
    	if (null == cursorExist)
    		throw new DScabiException("result set is null. No Compute Host with status as Available", "CMH.GMY.1");
    	int k = 0;
    	long n = 0;
    	n = m_table.count(document);
    	log.debug("n : {}", n);
    	//DMComputeServer cma[] = new DMComputeServer[(int)n];
    	List<DMComputeServer> cma = new ArrayList<DMComputeServer>();
	    while (cursorExist.hasNext()) {
	    	DDocument ob = cursorExist.next();
	    	log.debug("getMany() result from ob {}", ob.toString());
			String f = ob.getString("ComputeHost");
			if (null == f) {
	    		throw new DScabiException("Field name " + "ComputeHost" + " doesn't exist in dbobject in dbcursor.", "CMH.GMY.3");
			}
	    	log.debug("getMany() ComputeHost : {}", f);
			String f2 = ob.getString("ComputePort");
			if (null == f2) {
	    		throw new DScabiException("Field name " + "ComputePort" + " doesn't exist in dbobject in dbcursor.", "CMH.GMY.4");
			}
	    	log.debug("getMany() ComputePort : {}", f2);
			String f3 = ob.getString("MAXCSTHREADS");
			if (null == f3) {
	    		throw new DScabiException("Field name " + "MAXCSTHREADS" + " doesn't exist in dbobject in dbcursor.", "CMH.GMY.5");
			}
	    	log.debug("getMany() MAXCSTHREADS : {}", f3);

	    	//cma[k] = new DMComputeServer(m_ddb, f, f2);
	    	cma.add(new DMComputeServer(m_ddb, f, f2, f3));
	    	k++;
	    }
	    /*
	    DMComputeServer cma2[] = new DMComputeServer[k];
    	for (int i = 0; i < k; i++) {
    		cma2[i] = cma[i];
    		log.debug("getMany() cma i={} : {}", cma[i]); 
    		log.debug("getMany() cma2 i={} : {}", cma2[i]); 
    		
    		
    	}
	    return cma2; 
	    */
	    return cma;
	}

	public int exclude(List<DMComputeServer> cma, String jsonStrExclude) throws IOException {
		DMJson dmjson = new DMJson(jsonStrExclude);
		Set<String> st = dmjson.keySet();
		
		for (DMComputeServer cm : cma) {
			for (String s : st) {
				if (cm.toString().equals(dmjson.getString(s))) {
					log.debug("cm.toString() : {}", cm.toString());
					log.debug("dmjson.getString(s) : {}", dmjson.getString(s));
					cma.remove(cm);
				}
			} // End for
		} // End for

		return 0;
	}
	
	public int removeAll() throws DScabiException {
		m_table.removeAll();
		return 0;
	}
	
	public int checkIfRunningAndRemove() throws DScabiException {
		//DMComputeServer cma[];
		List<DMComputeServer> cma = null;
		try {
		cma = getAllAvailable();
		} catch (DScabiException e) {
			//e.printStackTrace();
			return 0;
		}
		boolean status = false;
		for (DMComputeServer cm : cma) {
			try {
				status = cm.checkIfRunning();
			}  catch (Error | RuntimeException e) {
				//e.printStackTrace();
				cm.remove();
				continue;
			} catch (Exception e) {
				//e.printStackTrace();
				cm.remove();
				continue;
			} catch (Throwable e) {
				//e.printStackTrace();
				cm.remove();
				continue;
			}
			if (false == status) {
				cm.remove();
			}
			
		}
		
		return 0;
	}
}
