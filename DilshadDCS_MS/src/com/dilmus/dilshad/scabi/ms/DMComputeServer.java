/**
 * @author Dilshad Mustafa
 * Copyright (c) Dilshad Mustafa
 * All Rights Reserved.
 * Created 26-Jan-2016
 * File Name : DMComputeServer.java
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

package com.dilmus.dilshad.scabi.ms;

import java.io.IOException;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dilmus.dilshad.scabi.common.DMJson;
import com.dilmus.dilshad.scabi.common.DMJsonHelper;
import com.dilmus.dilshad.scabi.common.DScabiException;
import com.dilmus.dilshad.scabi.core.DScabiClientException;
import com.dilmus.dilshad.scabi.db.DDB;
import com.dilmus.dilshad.scabi.db.DDocument;
import com.dilmus.dilshad.scabi.db.DResultSet;
import com.dilmus.dilshad.scabi.db.DTable;
import com.dilmus.dilshad.scabi.deprecated.DDBOld;
import com.dilmus.dilshad.scabi.deprecated.DObject;
import com.dilmus.dilshad.scabi.deprecated.DResultSetOld;
import com.dilmus.dilshad.scabi.deprecated.DTableOld;

/**
 * @author Dilshad Mustafa
 *
 */
public class DMComputeServer {

	final static Logger log = LoggerFactory.getLogger(DMComputeServer.class);
	private DDB m_ddb = null;
	private DTable m_table = null;
	private String m_fullHostName = null;
	private String m_port = null;
	private DDocument m_document = null;
	
	private CloseableHttpClient m_httpClient = null;
	private HttpHost m_target = null;
	
	private String m_maxCSThreads = null;
	
	public DMComputeServer(DDB ddb, String fullHostName, String port, String maxCSThreads) throws DScabiException {
		m_ddb = ddb;
		m_table = ddb.getTable("ComputeMetaDataTable");
		m_fullHostName = fullHostName;
		m_port = port;
		m_maxCSThreads = maxCSThreads;
		
		m_document = new DDocument();
		m_document.put("ComputeHost", fullHostName);
		m_document.put("ComputePort", port);
		m_document.put("MAXCSTHREADS", maxCSThreads);
	}
	
	public DMComputeServer(DDB ddb, String jsonString) throws IOException, DScabiException {
		m_ddb = ddb;
		m_table = ddb.getTable("ComputeMetaDataTable");
		DMJson djson = new DMJson(jsonString);
		m_fullHostName = djson.getString("ComputeHost");
		m_port = djson.getString("ComputePort");
		m_maxCSThreads = djson.getString("MAXCSTHREADS");
		
		m_document = new DDocument();
		m_document.put("ComputeHost", m_fullHostName);
		m_document.put("ComputePort", m_port);
		m_document.put("MAXCSTHREADS", m_maxCSThreads);
	}
	
	public int updateStatus(String status) throws DScabiException {
		long n = 0;
    	
    	//DResultSet cursorExist = m_table.find(m_document);
    	n = m_table.count(m_document);
    	if (1 == n) {
			log.debug("updateStatus() Inside 1 == n");
   			// Update
    	    DDocument newDocument = new DDocument();
    	   	newDocument.put("Status", status); // Available, Inuse, Hold, Blocked

    	   	DDocument updateObj = new DDocument();
    	   	updateObj.put("$set", newDocument);

    	   	m_table.update(m_document, updateObj);
    	}  else if (0 == n) {
    		log.debug("updateStatus() No matches found for ComputeHost {}, ComputePort {}", m_fullHostName, m_port);
			throw new DScabiException("No matches found for ComputeHost, ComputePort", "DCM.USS.1");
    	} else {
			log.debug("updateStatus() Multiple matches found for ComputeHost {}, ComputePort {}", m_fullHostName, m_port);
			throw new DScabiException("Multiple matches found for ComputeHost, ComputePort", "DCM.USS.2");
    	}
    	return 0;
	}
	
	public int remove() throws DScabiException {
		m_table.remove(m_document);
		return 0;
	}
	
	@Override
	public String toString() {
		return DMJsonHelper.computeHostPort(m_fullHostName, m_port, m_maxCSThreads);
	}
	
	public boolean checkIfRunning() throws IOException, DScabiClientException {
		
		boolean status = false;
		
		try {
			m_httpClient = HttpClientBuilder.create().build();
			m_target = new HttpHost(m_fullHostName, Integer.parseInt(m_port), "http");
    		status = true;
    	} catch (Exception e) {
			e.printStackTrace();
			status = false;
    		if (null != m_httpClient) { 
    			m_httpClient.close();
    			m_httpClient = null;
    		}
    		throw e;
		}
	
		HttpPost postRequest = new HttpPost("/Compute/isRunning");
		String myString = "";
	    StringEntity params =new StringEntity(myString);
	    
	    postRequest.addHeader("content-type", "application/json");
	    postRequest.setEntity(params);
	            			
		log.debug("checkIfRunning() executing request to " + m_target + "/Compute/isRunning");

		// works HttpResponse httpResponse = httpClient.execute(target, getRequest);
		HttpResponse httpResponse = m_httpClient.execute(m_target, postRequest);
		HttpEntity entity = httpResponse.getEntity();
	
		log.debug("checkIfRunning()----------------------------------------");
		log.debug("checkIfRunning() {}",httpResponse.getStatusLine());
		Header[] headers = httpResponse.getAllHeaders();
		for (int i = 0; i < headers.length; i++) {
			log.debug("checkIfRunning() {}", headers[i]);
		}
		log.debug("checkIfRunning()----------------------------------------");

		String jsonString = null;
		if (entity != null) {
			jsonString = EntityUtils.toString(entity);
			log.debug("checkIfRunning() {}", jsonString);
		}
		if (null == jsonString) {
			status = false;
			throw new DScabiClientException("Response is null for checkIfRunning()", "MEA.VAE.1");
		}
		if (DMJsonHelper.isError(jsonString)) {
			status = false;
			throw new DScabiClientException("Unable to checkIfRunning. Error message : " + jsonString, "MEA.VAE.2");
		}
		if (false == DMJsonHelper.isOk(jsonString))
			status = false;
		else
			status = true;
		if (null != m_httpClient) { 
			m_httpClient.close();
			m_httpClient = null;
		}
		
		return status;
	
	}
		
}
