/**
 * @author Dilshad Mustafa
 * Copyright (c) Dilshad Mustafa
 * All Rights Reserved.
 * Created 27-Jan-2016
 * File Name : DMeta.java
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

package com.dilmus.dilshad.scabi.core;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
//import org.apache.http.ParseException;
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
import com.dilmus.dilshad.scabi.core.compute.DComputeNoBlock;
import com.dilmus.dilshad.scabi.core.computesync_D1.DComputeBlock_D1;

/**
 * @author Dilshad Mustafa
 *
 */
public class DMeta {

	private final Logger log = LoggerFactory.getLogger(DMeta.class);
	private CloseableHttpClient m_httpClient = null;
	private HttpHost m_target = null;
	private String m_host = null;
	private String m_port = null;
	private boolean m_firstTime = true;
	
	public DMeta(String host, String port) throws IOException {
		m_host = host;
		m_port = port;
		m_firstTime = true;
	}
	
	public boolean isRunning() throws IOException, DScabiClientException {
		boolean status = false;
		try {
			m_httpClient = HttpClientBuilder.create().build();
			m_target = new HttpHost(m_host, Integer.parseInt(m_port), "http");
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
	
		HttpPost postRequest = new HttpPost("/Meta/isRunning");
		String myString = "";
	    StringEntity params =new StringEntity(myString);
	    
	    postRequest.addHeader("content-type", "application/json");
	    postRequest.setEntity(params);
	            			
		log.debug("isRunning() executing request to " + m_target + "/Meta/isRunning");

		// works HttpResponse httpResponse = httpClient.execute(target, getRequest);
		HttpResponse httpResponse = m_httpClient.execute(m_target, postRequest);
		HttpEntity entity = httpResponse.getEntity();
	
		log.debug("isRunning()----------------------------------------");
		log.debug("isRunning() {}",httpResponse.getStatusLine());
		Header[] headers = httpResponse.getAllHeaders();
		for (int i = 0; i < headers.length; i++) {
			log.debug("isRunning() {}", headers[i]);
		}
		log.debug("isRunning()----------------------------------------");

		String jsonString = null;
		if (entity != null) {
			jsonString = EntityUtils.toString(entity);
			log.debug("isRunning() {}", jsonString);
		}
		if (null == jsonString) {
			status = false;
			throw new DScabiClientException("Response is null for isRunning()", "MEA.VAE.1");
		}
		if (DMJsonHelper.isError(jsonString)) {
			status = false;
			throw new DScabiClientException("Unable to check isRunning. Error message : " + jsonString, "MEA.VAE.2");
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

	public boolean open() throws IOException {
		if (false == m_firstTime)
			return true;
		boolean status = false;
		try {
			m_httpClient = HttpClientBuilder.create().build();
			m_target = new HttpHost(m_host, Integer.parseInt(m_port), "http");
    		status = true;
    		m_firstTime = false;
    	} catch (Exception e) {
			e.printStackTrace();
			status = false;
    		if (null != m_httpClient) { 
    			m_httpClient.close();
    			m_httpClient = null;
    		}
    		throw e;
    	}
		return status;
	}
	
	public boolean close() throws IOException {
		if (null != m_httpClient) 
			m_httpClient.close();

		m_httpClient = null;
		m_target = null;
		m_firstTime = true;
		return true;
	}
	
	public String getHost() {
		return m_host;
	}
	
	public String getPort() {
		return m_port;
	}
	
	
	public DComputeBlock_D1 computeAlloc() throws /*ParseException,*/ IOException, DScabiClientException {
		open();
		HttpPost postRequest = new HttpPost("/Meta/Compute/Alloc");
		String myString = "";
	    StringEntity params =new StringEntity(myString);
	    
	    postRequest.addHeader("content-type", "application/json");
	    postRequest.setEntity(params);
	            			
		log.debug("computeAlloc() executing request to " + m_target + "/Meta/Compute/Alloc");

		// works HttpResponse httpResponse = httpClient.execute(target, getRequest);
		HttpResponse httpResponse = m_httpClient.execute(m_target, postRequest);
		HttpEntity entity = httpResponse.getEntity();
	
		log.debug("computeAlloc()----------------------------------------");
		log.debug("computeAlloc() {}",httpResponse.getStatusLine());
		Header[] headers = httpResponse.getAllHeaders();
		for (int i = 0; i < headers.length; i++) {
			log.debug("computeAlloc() {}", headers[i]);
		}
		log.debug("computeAlloc()----------------------------------------");

		String jsonString = null;
		if (entity != null) {
			jsonString = EntityUtils.toString(entity);
			log.debug("computeAlloc() {}", jsonString);
		}
		if (null == jsonString) {
			throw new DScabiClientException("Response is null for computeAlloc()", "MEA.CAC.1");
		}
		if (DMJsonHelper.isError(jsonString)) {
			throw new DScabiClientException("Unable to alloc compute unit. Error message : " + jsonString, "MEA.CAC.2");
		}
		return new DComputeBlock_D1(jsonString);
	}
	
	
	public int computeRegister(String computeHost, String computePort, String maxCSThreads) throws /*ParseException,*/ IOException, DScabiClientException {
		open();
		HttpPost postRequest = new HttpPost("/Meta/Compute/Register");
		String myString = "{ \"ComputeHost\" : \"" + computeHost + "\", \"ComputePort\" : \"" + computePort + "\", \"MAXCSTHREADS\" : \"" +  maxCSThreads + "\" }";
	    StringEntity params =new StringEntity(myString);
	    
	    postRequest.addHeader("content-type", "application/json");
	    postRequest.setEntity(params);
	            			
		log.debug("computeRegister() executing request to " + m_target + "/Meta/Compute/Register");

		// works HttpResponse httpResponse = httpClient.execute(target, getRequest);
		HttpResponse httpResponse = m_httpClient.execute(m_target, postRequest);
		HttpEntity entity = httpResponse.getEntity();
	
		log.debug("computeRegister()----------------------------------------");
		log.debug("computeRegister() {}",httpResponse.getStatusLine());
		Header[] headers = httpResponse.getAllHeaders();
		for (int i = 0; i < headers.length; i++) {
			log.debug("computeRegister() {}", headers[i]);
		}
		log.debug("computeRegister()----------------------------------------");

		String jsonString = null;
		if (entity != null) {
			jsonString = EntityUtils.toString(entity);
			log.debug("computeRegister() {}", jsonString);
		}
		if (null == jsonString) {
			throw new DScabiClientException("Response is null for computeRegister()", "MEA.CAC.1");
		}
		if (DMJsonHelper.isError(jsonString)) {
			throw new DScabiClientException("Unable to register compute unit. Error message : " + jsonString, "MEA.CAC.2");
		}
		if (false == DMJsonHelper.isOk(jsonString)) {
			throw new DScabiClientException("Unable to register compute unit. Response is not ok. Error message : " + jsonString, "MEA.CAC.2");
		}
		return 0;
	}

	/* Reference - single object return
	public DComputeSync getCompute() throws ParseException, IOException, DScabiClientException {
		open();
		HttpPost postRequest = new HttpPost("/Meta/Compute/GetOne");
		String myString = "";
	    StringEntity params =new StringEntity(myString);
	    
	    postRequest.addHeader("content-type", "application/json");
	    postRequest.setEntity(params);
	            			
		log.debug("computeAlloc() executing request to " + m_target + "/Meta/Compute/GetOne");

		// works HttpResponse httpResponse = httpClient.execute(target, getRequest);
		HttpResponse httpResponse = m_httpClient.execute(m_target, postRequest);
		HttpEntity entity = httpResponse.getEntity();
	
		log.debug("computeAlloc()----------------------------------------");
		log.debug("computeAlloc() {}",httpResponse.getStatusLine());
		Header[] headers = httpResponse.getAllHeaders();
		for (int i = 0; i < headers.length; i++) {
			log.debug("computeAlloc() {}", headers[i]);
		}
		log.debug("computeAlloc()----------------------------------------");

		String jsonString = null;
		if (entity != null) {
			jsonString = EntityUtils.toString(entity);
			log.debug("computeAlloc() {}", jsonString);
		}
		if (null == jsonString) {
			throw new DScabiClientException("Response is null for computeAlloc()", "MEA.CAC.1");
		}
		if (DMJsonHelper.isError(jsonString)) {
			throw new DScabiClientException("Unable to alloc compute unit. Error message : " + jsonString, "MEA.CAC.2");
		}
		return new DComputeSync(jsonString);
	}
	*/
	
	public List<DComputeBlock_D1> getComputeManyMayExclude(int howMany, List<DComputeBlock_D1> exclude) throws /*ParseException,*/ IOException, DScabiClientException {
		open();
		HttpPost postRequest = new HttpPost("/Meta/Compute/GetManyMayExclude");
		DMJson dmjson = new DMJson("GetComputeMany", "" + howMany);
		DMJson dmjsonExclude = null;
		int k = 1;
		for (DComputeBlock_D1 cb : exclude) {
			if (null == dmjsonExclude) {
				dmjsonExclude = new DMJson("" + k, cb.toString());
			} else {
				dmjsonExclude = dmjsonExclude.add("" + k, cb.toString());
			}
		}
		dmjson = dmjson.add("ComputeExclude", dmjsonExclude.toString());
		StringEntity params =new StringEntity(dmjson.toString());
		log.debug("getComputeManyMayExclude() dmjson.toString() : {}", dmjson.toString());

	    postRequest.addHeader("content-type", "application/json");
	    postRequest.setEntity(params);
	            			
		log.debug("getComputeManyMayExclude() executing request to " + m_target + "/Meta/Compute/GetManyMayExclude");

		// works HttpResponse httpResponse = httpClient.execute(target, getRequest);
		HttpResponse httpResponse = m_httpClient.execute(m_target, postRequest);
		HttpEntity entity = httpResponse.getEntity();
	
		log.debug("getComputeManyMayExclude()----------------------------------------");
		log.debug("getComputeManyMayExclude() {}",httpResponse.getStatusLine());
		Header[] headers = httpResponse.getAllHeaders();
		for (int i = 0; i < headers.length; i++) {
			log.debug("getComputeManyMayExclude() {}", headers[i]);
		}
		log.debug("getComputeManyMayExclude()----------------------------------------");

		String jsonString = null;
		if (entity != null) {
			jsonString = EntityUtils.toString(entity);
			log.debug("getComputeManyMayExclude() {}", jsonString);
		}
		if (null == jsonString) {
			throw new DScabiClientException("Response is null for getCompute(int howmany)", "MEA.CAC.1");
		}
		if (DMJsonHelper.isError(jsonString)) {
			throw new DScabiClientException("Unable to get compute unit. Error message : " + jsonString, "MEA.CAC.2");
		}
		DMJson djson = new DMJson(jsonString);
		long count = djson.getCount();
		Set<String> st = djson.keySet();
		// st.remove("Count"); // UnsupportedOperationException, Unmodifiable
		if (0 == count)
			return null;
		List<DComputeBlock_D1> cba = new LinkedList<DComputeBlock_D1>();
		for (String s : st) {
			if (s.equals("Count"))
				continue;
			cba.add(new DComputeBlock_D1(djson.getString(s)));
		}
		
		return cba;
	}
	
	public List<DComputeBlock_D1> getComputeMany(int howMany) throws /*ParseException,*/ IOException, DScabiClientException {
		open();
		HttpPost postRequest = new HttpPost("/Meta/Compute/GetMany");
		String myString = "{ \"GetComputeMany\" : \"" + howMany + "\" }";
	    StringEntity params =new StringEntity(myString);
	    
	    postRequest.addHeader("content-type", "application/json");
	    postRequest.setEntity(params);
	            			
		log.debug("getComputeMany() executing request to " + m_target + "/Meta/Compute/GetMany");

		// works HttpResponse httpResponse = httpClient.execute(target, getRequest);
		HttpResponse httpResponse = m_httpClient.execute(m_target, postRequest);
		HttpEntity entity = httpResponse.getEntity();
	
		log.debug("getComputeMany()----------------------------------------");
		log.debug("getComputeMany() {}",httpResponse.getStatusLine());
		Header[] headers = httpResponse.getAllHeaders();
		for (int i = 0; i < headers.length; i++) {
			log.debug("getComputeMany() {}", headers[i]);
		}
		log.debug("getComputeMany()----------------------------------------");

		String jsonString = null;
		if (entity != null) {
			jsonString = EntityUtils.toString(entity);
			log.debug("getComputeMany() {}", jsonString);
		}
		if (null == jsonString) {
			throw new DScabiClientException("Response is null for getCompute(int howmany)", "MEA.CAC.1");
		}
		if (DMJsonHelper.isError(jsonString)) {
			throw new DScabiClientException("Unable to get compute unit. Error message : " + jsonString, "MEA.CAC.2");
		}
		DMJson djson = new DMJson(jsonString);
		long count = djson.getCount();
		Set<String> st = djson.keySet();
		// st.remove("Count"); // UnsupportedOperationException, Unmodifiable
		if (0 == count)
			return null;
		List<DComputeBlock_D1> cba = new LinkedList<DComputeBlock_D1>();
		//DComputeBlock cba[] = new DComputeBlock[count];
		//int i = 0;
		for (String s : st) {
			if (s.equals("Count"))
				continue;
			cba.add(new DComputeBlock_D1(djson.getString(s)));
			//cba[i] = new DComputeBlock(djson.getString(s));
			//i++;
		}
		
		return cba;
	}

	public String getComputeManyJsonStr(long howMany) throws /*ParseException,*/ IOException, DScabiClientException {
		open();
		HttpPost postRequest = new HttpPost("/Meta/Compute/GetMany");
		String myString = "{ \"GetComputeMany\" : \"" + howMany + "\" }";
	    StringEntity params =new StringEntity(myString);
	    
	    postRequest.addHeader("content-type", "application/json");
	    postRequest.setEntity(params);
	            			
		log.debug("getComputeManyJsonStr() executing request to " + m_target + "/Meta/Compute/GetMany");

		// works HttpResponse httpResponse = httpClient.execute(target, getRequest);
		HttpResponse httpResponse = m_httpClient.execute(m_target, postRequest);
		HttpEntity entity = httpResponse.getEntity();
	
		log.debug("getComputeManyJsonStr()----------------------------------------");
		log.debug("getComputeManyJsonStr() {}",httpResponse.getStatusLine());
		Header[] headers = httpResponse.getAllHeaders();
		for (int i = 0; i < headers.length; i++) {
			log.debug("getComputeManyJsonStr() {}", headers[i]);
		}
		log.debug("getComputeManyJsonStr()----------------------------------------");

		String jsonString = null;
		if (entity != null) {
			jsonString = EntityUtils.toString(entity);
			log.debug("getComputeManyJsonStr() {}", jsonString);
		}
		if (null == jsonString) {
			throw new DScabiClientException("Response is null for getCompute(int howmany)", "MEA.CAC.1");
		}
		if (DMJsonHelper.isError(jsonString)) {
			throw new DScabiClientException("Unable to get compute unit. Error message : " + jsonString, "MEA.CAC.2");
		}
		
		return jsonString;
	}
	
	public List<DComputeNoBlock> getComputeNoBlockMany(int howMany) throws /*ParseException,*/ IOException, DScabiClientException {
		open();
		HttpPost postRequest = new HttpPost("/Meta/Compute/GetMany");
		String myString = "{ \"GetComputeMany\" : \"" + howMany + "\" }";
	    StringEntity params =new StringEntity(myString);
	    
	    postRequest.addHeader("content-type", "application/json");
	    postRequest.setEntity(params);
	            			
		log.debug("getComputeNoBlockMany() executing request to " + m_target + "/Meta/Compute/GetMany");

		// works HttpResponse httpResponse = httpClient.execute(target, getRequest);
		HttpResponse httpResponse = m_httpClient.execute(m_target, postRequest);
		HttpEntity entity = httpResponse.getEntity();
	
		log.debug("getComputeNoBlockMany()----------------------------------------");
		log.debug("getComputeNoBlockMany() {}",httpResponse.getStatusLine());
		Header[] headers = httpResponse.getAllHeaders();
		for (int i = 0; i < headers.length; i++) {
			log.debug("getComputeNoBlockMany() {}", headers[i]);
		}
		log.debug("getComputeNoBlockMany()----------------------------------------");

		String jsonString = null;
		if (entity != null) {
			jsonString = EntityUtils.toString(entity);
			log.debug("getComputeNoBlockMany() {}", jsonString);
		}
		if (null == jsonString) {
			throw new DScabiClientException("Response is null for getCompute(int howmany)", "MEA.CAC.1");
		}
		if (DMJsonHelper.isError(jsonString)) {
			throw new DScabiClientException("Unable to get compute unit. Error message : " + jsonString, "MEA.CAC.2");
		}
		DMJson djson = new DMJson(jsonString);
		long count = djson.getCount();
		Set<String> st = djson.keySet();
		// st.remove("Count"); // UnsupportedOperationException, Unmodifiable
		if (0 == count)
			return null;
		List<DComputeNoBlock> csa = new LinkedList<DComputeNoBlock>();
		//DComputeNoBlock csa[] = new DComputeNoBlock[count];
		//int i = 0;
		for (String s : st) {
			if (s.equals("Count"))
				continue;
			csa.add(new DComputeNoBlock(djson.getString(s)));
			//csa[i] = new DComputeNoBlock(djson.getString(s));
			//i++;
		}
		
		return csa;
	}

	public String getComputeNoBlockManyJsonStr(long howMany) throws /*ParseException,*/ IOException, DScabiClientException {
		open();
		HttpPost postRequest = new HttpPost("/Meta/Compute/GetMany");
		String myString = "{ \"GetComputeMany\" : \"" + howMany + "\" }";
	    StringEntity params =new StringEntity(myString);
	    
	    postRequest.addHeader("content-type", "application/json");
	    postRequest.setEntity(params);
	            			
		log.debug("getComputeNoBlockManyJsonStr() executing request to " + m_target + "/Meta/Compute/GetMany");

		// works HttpResponse httpResponse = httpClient.execute(target, getRequest);
		HttpResponse httpResponse = m_httpClient.execute(m_target, postRequest);
		HttpEntity entity = httpResponse.getEntity();
	
		log.debug("getComputeNoBlockManyJsonStr()----------------------------------------");
		log.debug("getComputeNoBlockManyJsonStr() {}",httpResponse.getStatusLine());
		Header[] headers = httpResponse.getAllHeaders();
		for (int i = 0; i < headers.length; i++) {
			log.debug("getComputeNoBlockManyJsonStr() {}", headers[i]);
		}
		log.debug("getComputeNoBlockManyJsonStr()----------------------------------------");

		String jsonString = null;
		if (entity != null) {
			jsonString = EntityUtils.toString(entity);
			log.debug("getComputeNoBlockManyJsonStr() {}", jsonString);
		}
		if (null == jsonString) {
			throw new DScabiClientException("Response is null for getCompute(int howmany)", "MEA.CAC.1");
		}
		if (DMJsonHelper.isError(jsonString)) {
			throw new DScabiClientException("Unable to get Compute Server details. Error message : " + jsonString, "MEA.CAC.2");
		}
		
		return jsonString;
	}

	public List<DComputeNoBlock> getComputeNoBlockManyMayExclude(int howMany, List<DComputeNoBlock> exclude) throws /*ParseException,*/ IOException, DScabiClientException {
		open();
		HttpPost postRequest = new HttpPost("/Meta/Compute/GetManyMayExclude");
		DMJson dmjson = new DMJson("GetComputeMany", "" + howMany);
		DMJson dmjsonExclude = null;
		int k = 1;
		for (DComputeNoBlock cnb : exclude) {
			if (null == dmjsonExclude) {
				dmjsonExclude = new DMJson("" + k, cnb.toString());
			} else {
				dmjsonExclude = dmjsonExclude.add("" + k, cnb.toString());
			}
		}
		dmjson = dmjson.add("ComputeExclude", dmjsonExclude.toString());
		StringEntity params =new StringEntity(dmjson.toString());
	   	log.debug("getComputeNoBlockManyMayExclude() dmjson.toString() : {}", dmjson.toString());
	   	
	    postRequest.addHeader("content-type", "application/json");
	    postRequest.setEntity(params);
	            			
		log.debug("getComputeNoBlockManyMayExclude() executing request to " + m_target + "/Meta/Compute/GetManyMayExclude");

		// works HttpResponse httpResponse = httpClient.execute(target, getRequest);
		HttpResponse httpResponse = m_httpClient.execute(m_target, postRequest);
		HttpEntity entity = httpResponse.getEntity();
	
		log.debug("getComputeNoBlockManyMayExclude()----------------------------------------");
		log.debug("getComputeNoBlockManyMayExclude() {}",httpResponse.getStatusLine());
		Header[] headers = httpResponse.getAllHeaders();
		for (int i = 0; i < headers.length; i++) {
			log.debug("getComputeNoBlockManyMayExclude() {}", headers[i]);
		}
		log.debug("getComputeNoBlockManyMayExclude()----------------------------------------");

		String jsonString = null;
		if (entity != null) {
			jsonString = EntityUtils.toString(entity);
			log.debug("getComputeNoBlockManyMayExclude() {}", jsonString);
		}
		if (null == jsonString) {
			throw new DScabiClientException("Response is null for getCompute(int howmany)", "MEA.CAC.1");
		}
		if (DMJsonHelper.isError(jsonString)) {
			throw new DScabiClientException("Unable to get compute unit. Error message : " + jsonString, "MEA.CAC.2");
		}
		DMJson djson = new DMJson(jsonString);
		long count = djson.getCount();
		Set<String> st = djson.keySet();
		// st.remove("Count"); // UnsupportedOperationException, Unmodifiable
		if (0 == count)
			return null;
		List<DComputeNoBlock> csa = new LinkedList<DComputeNoBlock>();
		for (String s : st) {
			if (s.equals("Count"))
				continue;
			csa.add(new DComputeNoBlock(djson.getString(s)));
		}
		
		return csa;
	}

	public String namespaceRegister(Dson dson) throws /*ParseException,*/ IOException, DScabiClientException {
		open();
		HttpPost postRequest = new HttpPost("/Meta/Namespace/Register");
		String myString = dson.toString();
	    StringEntity params =new StringEntity(myString);
	    
	    postRequest.addHeader("content-type", "application/json");
	    postRequest.setEntity(params);
	            			
		log.debug("namespaceRegister() executing request to " + m_target + "/Meta/Namespace/Register");

		// works HttpResponse httpResponse = httpClient.execute(target, getRequest);
		HttpResponse httpResponse = m_httpClient.execute(m_target, postRequest);
		HttpEntity entity = httpResponse.getEntity();
	
		log.debug("namespaceRegister()----------------------------------------");
		log.debug("namespaceRegister() {}",httpResponse.getStatusLine());
		Header[] headers = httpResponse.getAllHeaders();
		for (int i = 0; i < headers.length; i++) {
			log.debug("namespaceRegister() {}", headers[i]);
		}
		log.debug("namespaceRegister()----------------------------------------");

		String jsonString = null;
		if (entity != null) {
			jsonString = EntityUtils.toString(entity);
			log.debug("namespaceRegister() {}", jsonString);
		}
		if (null == jsonString) {
			throw new DScabiClientException("Response is null for namespaceRegister()", "MEA.CAC.1");
		}
		if (DMJsonHelper.isError(jsonString)) {
			throw new DScabiClientException("Unable to register namespace. Error message : " + jsonString, "MEA.CAC.2");
		}
		if (false == DMJsonHelper.isResult(jsonString)) {
			throw new DScabiClientException("Unable to register namespace. Response is not ok. Error message : " + jsonString, "MEA.CAC.2");
		}
		return jsonString;
	}

	public boolean namespaceExists(String strNamespace) throws /*ParseException,*/ IOException, DScabiClientException, DScabiException {
		open();
		HttpPost postRequest = new HttpPost("/Meta/Namespace/isExist");
		String myString = "{ \"Namespace\" : \"" + strNamespace + "\" }";
	    StringEntity params =new StringEntity(myString);
	    
	    postRequest.addHeader("content-type", "application/json");
	    postRequest.setEntity(params);
	            			
		log.debug("namespaceExists() executing request to " + m_target + "/Meta/Namespace/isExist");

		// works HttpResponse httpResponse = httpClient.execute(target, getRequest);
		HttpResponse httpResponse = m_httpClient.execute(m_target, postRequest);
		HttpEntity entity = httpResponse.getEntity();
	
		log.debug("namespaceExists()----------------------------------------");
		log.debug("namespaceExists() {}",httpResponse.getStatusLine());
		Header[] headers = httpResponse.getAllHeaders();
		for (int i = 0; i < headers.length; i++) {
			log.debug("namespaceExists() {}", headers[i]);
		}
		log.debug("namespaceExists()----------------------------------------");

		String jsonString = null;
		if (entity != null) {
			jsonString = EntityUtils.toString(entity);
			log.debug("namespaceExists() {}", jsonString);
		}
		if (null == jsonString) {
			throw new DScabiClientException("Response is null for namespace " + strNamespace, "MEA.GNE.1");
		}
		if (DMJsonHelper.isError(jsonString)) {
			throw new DScabiClientException("Unable to check for namespace " + strNamespace + " Error message : " + jsonString, "MEA.GNE.2");
		}
		if (DMJsonHelper.isTrue(jsonString)) {
			return true;
		} else if (DMJsonHelper.isFalse(jsonString))
			return false;
		else
			throw new DScabiClientException("Unable to check for namespace " + strNamespace + " Error message : " + jsonString, "MEA.GNE.2");
		
	}

	public DNamespace getNamespace(String strNamespace) throws /*ParseException,*/ IOException, DScabiClientException, DScabiException {
		open();
		HttpPost postRequest = new HttpPost("/Meta/Namespace/Get");
		String myString = "{ \"Namespace\" : \"" + strNamespace + "\" }";
	    StringEntity params =new StringEntity(myString);
	    
	    postRequest.addHeader("content-type", "application/json");
	    postRequest.setEntity(params);
	            			
		log.debug("getNamespace() executing request to " + m_target + "/Meta/Namespace/Get");

		// works HttpResponse httpResponse = httpClient.execute(target, getRequest);
		HttpResponse httpResponse = m_httpClient.execute(m_target, postRequest);
		HttpEntity entity = httpResponse.getEntity();
	
		log.debug("getNamespace()----------------------------------------");
		log.debug("getNamespace() {}",httpResponse.getStatusLine());
		Header[] headers = httpResponse.getAllHeaders();
		for (int i = 0; i < headers.length; i++) {
			log.debug("getNamespace() {}", headers[i]);
		}
		log.debug("getNamespace()----------------------------------------");

		String jsonString = null;
		if (entity != null) {
			jsonString = EntityUtils.toString(entity);
			log.debug("getNamespace() {}", jsonString);
		}
		if (null == jsonString) {
			throw new DScabiClientException("Response is null for namespace " + strNamespace, "MEA.GNE.1");
		}
		if (DMJsonHelper.isError(jsonString)) {
			throw new DScabiClientException("Unable to get namespace " + strNamespace + " Error message : " + jsonString, "MEA.GNE.2");
		}
		DNamespace namespace = new DNamespace(jsonString);
		return namespace;
		
	}

	public DNamespace getNamespace(String strNamespace, String metaType) throws /*ParseException,*/ IOException, DScabiClientException, DScabiException {
		open();
		HttpPost postRequest = new HttpPost("/Meta/Namespace/GetByQuery");
		
		String myString = "{ \"Type\" : \"" +  metaType + "\", \"Namespace\" : \"" + strNamespace + "\" }";
	    StringEntity params =new StringEntity(myString);
	    
	    postRequest.addHeader("content-type", "application/json");
	    postRequest.setEntity(params);
	            			
		log.debug("getNamespace() executing request to " + m_target + "/Meta/Namespace/Get");

		// works HttpResponse httpResponse = httpClient.execute(target, getRequest);
		HttpResponse httpResponse = m_httpClient.execute(m_target, postRequest);
		HttpEntity entity = httpResponse.getEntity();
	
		log.debug("getNamespace()----------------------------------------");
		log.debug("getNamespace() {}",httpResponse.getStatusLine());
		Header[] headers = httpResponse.getAllHeaders();
		for (int i = 0; i < headers.length; i++) {
			log.debug("getNamespace() {}", headers[i]);
		}
		log.debug("getNamespace()----------------------------------------");

		String jsonString = null;
		if (entity != null) {
			jsonString = EntityUtils.toString(entity);
			log.debug("getNamespace() {}", jsonString);
		}

		if (null == jsonString) {
			throw new DScabiClientException("Response is null for Type : " + metaType + " Namespace : " + strNamespace, "MEA.GNE2.1");
		}
		
		if (DMJsonHelper.isError(jsonString)) {
			throw new DScabiClientException("Unable to get namespace " + strNamespace + " for Type : " + metaType + " Error message : " + jsonString, "MEA.GNE2.2");
		}
		
		DNamespace namespace = new DNamespace(jsonString);
		return namespace;
	
	}
	
	private DNamespace findOneNamespace(String metaType) throws /*ParseException,*/ IOException, DScabiClientException, DScabiException {
		open();
		HttpPost postRequest = new HttpPost("/Meta/Namespace/FindOne");
		String myString = "{ \"Type\" : \"" +  metaType + "\" }";
	    StringEntity params =new StringEntity(myString);
	    
	    postRequest.addHeader("content-type", "application/json");
	    postRequest.setEntity(params);
	            			
		log.debug("findOneNamespace() executing request to " + m_target + "/Meta/Namespace/FindOne");

		// works HttpResponse httpResponse = httpClient.execute(target, getRequest);
		HttpResponse httpResponse = m_httpClient.execute(m_target, postRequest);
		HttpEntity entity = httpResponse.getEntity();
	
		log.debug("findOneNamespace()----------------------------------------");
		log.debug("findOneNamespace() {}",httpResponse.getStatusLine());
		Header[] headers = httpResponse.getAllHeaders();
		for (int i = 0; i < headers.length; i++) {
			log.debug("findOneNamespace() {}", headers[i]);
		}
		log.debug("findOneNamespace()----------------------------------------");

		String jsonString = null;
		if (entity != null) {
			jsonString = EntityUtils.toString(entity);
			log.debug("findOneNamespace() {}", jsonString);
		}
		if (null == jsonString) {
			throw new DScabiClientException("Response is null for Meta Type : " + metaType, "MEA.FON.1");
		}
		if (DMJsonHelper.isError(jsonString)) {
			throw new DScabiClientException("Unable to get namespace. Meta Type : " + metaType + " Error message : " + jsonString, "MEA.FON.2");
		}
		DNamespace namespace = new DNamespace(jsonString);
		return namespace;
		
	}

	public DNamespace findOneMetaThisNS() throws /*ParseException,*/ IOException, DScabiClientException, DScabiException {
		return findOneNamespace("MetaThis");
	}
	
	public DNamespace findOneMetaRemoteNS() throws /*ParseException,*/ IOException, DScabiClientException, DScabiException {
		return findOneNamespace("MetaRemote");
	}

	public DNamespace findOneAppTableNS() throws /*ParseException,*/ IOException, DScabiClientException, DScabiException {
		return findOneNamespace("AppTable");
	}
	
	public DNamespace findOneJavaFileNS() throws /*ParseException,*/ IOException, DScabiClientException, DScabiException {
		return findOneNamespace("JavaFile");
	}
	
	public DNamespace findOneFileNS() throws /*ParseException,*/ IOException, DScabiClientException, DScabiException {
		return findOneNamespace("File");
	}
	
}
