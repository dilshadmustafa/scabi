/**
 * @author Dilshad Mustafa
 * Copyright (c) Dilshad Mustafa
 * All Rights Reserved.
 * @since 25-Feb-2016
 * File Name : DComputeNoBlock.java
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

package com.dilmus.dilshad.scabi.core.async;

import java.io.IOException;

import org.apache.http.client.ClientProtocolException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.nio.protocol.HttpAsyncRequestConsumer;
import org.apache.http.util.Asserts;
import org.apache.http.util.EntityUtils;


import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Method;
import java.util.zip.GZIPOutputStream;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.ParseException;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dilmus.dilshad.scabi.common.DMClassLoader;
import com.dilmus.dilshad.scabi.common.DMJson;
import com.dilmus.dilshad.scabi.common.DMJsonHelper;
import com.dilmus.dilshad.scabi.common.DMUtil;
import com.dilmus.dilshad.scabi.common.DScabiException;
import com.dilmus.dilshad.scabi.core.DComputeUnit;
import com.dilmus.dilshad.scabi.core.DMeta;

import javax.json.JsonObject;

/**
 * @author Dilshad Mustafa
 *
 */
public class DComputeNoBlock {

	private static final Logger log = LoggerFactory.getLogger(DComputeNoBlock.class);
	private CloseableHttpAsyncClient m_httpClient = null;
	private String m_jsonString = null;
	private HttpHost m_target = null;
	private String m_computeHost = null;
	private String m_computePort = null;
	private DMJson m_djson = null;
	private String m_jsonStrInput = null;
	private HttpHost m_metaTarget = null;
	private DMeta m_meta = null;
	private int m_TU = 1;
	private int m_SU = 1;
	private boolean m_isFaulty = false;
	
	public int MAX_REQUESTS = 1000;
	private int m_countRequests = 0;
	private Object m_lockCountRequests = new Object();
	
	private List<String> m_jarFilePathList = null;
	
	private boolean m_isComputeUnitJarsSet = false;
	private DMClassLoader m_dcl = null;
	
	public int setComputeUnitJars(DMClassLoader dcl) {
		m_isComputeUnitJarsSet = true;
		m_dcl = dcl;
		return 0;
	}
	
	public DComputeNoBlock(String jsonString) throws IOException {

		m_djson = new DMJson(jsonString);
		m_computeHost = m_djson.getString("ComputeHost");
		m_computePort = m_djson.getString("ComputePort");

		try {
			m_httpClient = HttpAsyncClients.createDefault();
	        m_httpClient.start();
	        m_target = new HttpHost(m_computeHost, Integer.parseInt(m_computePort), "http");
    	} catch (Exception e) {
			//e.printStackTrace();
    		if (null != m_httpClient) 
    			m_httpClient.close();
    		throw e;
    	}

		m_jsonString = jsonString;
		m_jsonStrInput = DMJsonHelper.empty();
		
		m_isFaulty = false;
		
		m_jarFilePathList = new ArrayList<String>();
	}
	
	public DComputeNoBlock(DMeta meta) throws Exception {
		
		String jsonCompute = null;
		try {
			m_httpClient = HttpAsyncClients.createDefault();
			m_httpClient.start();
			Future<HttpResponse> futureHttpResponse = computeAlloc(meta);
			HttpResponse httpResponse = DComputeNoBlock.get(futureHttpResponse);
			jsonCompute = DComputeNoBlock.getResult(httpResponse);
			m_djson = new DMJson(jsonCompute);
			m_computeHost = m_djson.getString("ComputeHost");
			m_computePort = m_djson.getString("ComputePort");
			m_target = new HttpHost(m_computeHost, Integer.parseInt(m_computePort), "http");
			m_jsonString = jsonCompute;
			m_meta = meta;
			m_jsonStrInput = DMJsonHelper.empty();
			
		} catch (Exception e) {
			//e.printStackTrace();
			if (null != m_httpClient) 
				m_httpClient.close();
			throw e;
		}
		
		m_isFaulty = false;
		
		m_jarFilePathList = new ArrayList<String>();
	}
	
	public int close() throws IOException {
		if (null != m_httpClient) 
			m_httpClient.close();
		return 0;
	}
	
	public boolean isAllowed() {
		if (m_countRequests < MAX_REQUESTS)
			return true;
		else
			return false;
	}
	
	public int decCountRequests() {
		synchronized (m_lockCountRequests) {
			if (m_countRequests > 0)
				m_countRequests--;
		}
		return 0;
	}
	
	public int incCountRequests() {
		synchronized (m_lockCountRequests) {
			m_countRequests++;
		}
		return 0;
	}

	
	public static HttpResponse get(Future<HttpResponse> future) throws InterruptedException, ExecutionException, TimeoutException {
	    HttpResponse httpResponse = null;

	    httpResponse = future.get();
		
		return httpResponse;
	}
	
	public static String getResult(HttpResponse httpResponse) throws ParseException, IOException {
		HttpEntity entity = httpResponse.getEntity();
		
		// Debugging
		//log.debug("getResult()----------------------------------------");
		//log.debug("getResult() {}",httpResponse.getStatusLine());
		//Header[] headers = httpResponse.getAllHeaders();
		//for (int i = 0; i < headers.length; i++) {
		//	log.debug("getResult() {}", headers[i]);
		//}
		//log.debug("getResult()----------------------------------------");

		String jsonString = null;
		if (entity != null) {
			jsonString = EntityUtils.toString(entity);
			//log.debug("getResult() {}", jsonString);
		}
		
		if (null == jsonString)
			return DMJson.error("null");
		
		return jsonString;

	}
	
	public int setFaulty(boolean isFaulty) {
		m_isFaulty = isFaulty;
		return 0;
	}
	
	public boolean isFaulty() {
		return m_isFaulty;
	}
	
	private Future<HttpResponse> computeAlloc(DMeta meta) throws ParseException, IOException {
		
		m_metaTarget = new HttpHost(meta.getHost(), Integer.parseInt(meta.getPort()), "http");
		
		HttpPost postRequest = new HttpPost("/Meta/Compute/Alloc");
		String myString = "";
	    StringEntity params =new StringEntity(myString);
	    
	    postRequest.addHeader("content-type", "application/json");
	    postRequest.setEntity(params);
	            			
		log.debug("computeAlloc() executing request to " + m_metaTarget + "/Meta/Compute/Alloc");
		incCountRequests();
		Future<HttpResponse> futureHttpResponse = m_httpClient.execute(m_metaTarget, postRequest, null);
		return futureHttpResponse;
				
	}

	public String toString() {
		return m_jsonString;
	}
	
	public int setInput(String jsonInput) {
		m_jsonStrInput = jsonInput;
		return 0;
	}

	public int setTU(int tu) {
		m_TU = tu;
		return 0;
	}

	public int setSU(int su) {
		m_SU = su;
		return 0;
	}
	
	public int getTU() {
		return m_TU;
	}

	public int getSU() {
		return m_SU;
	}

	public Future<HttpResponse> executeCode(String bshSource) throws ClientProtocolException, IOException {
		HttpPost postRequest = new HttpPost("/Compute/Execute/BshCode");

		DMJson djson1 = new DMJson("TotalComputeUnit", "" + m_TU);
		DMJson djson2 = djson1.add("SplitComputeUnit", "" + m_SU);
		DMJson djson3 = djson2.add("JsonInput", "" + m_jsonStrInput);
		DMJson djson4 = djson3.add("BshSource", bshSource);

		log.debug("executeCode() m_jarFilePathList.size() : {}", m_jarFilePathList.size());
		if (m_jarFilePathList.size() > 0) {
			djson4 = addJars(djson4);
			m_jarFilePathList.clear();
		}
		
		log.debug("executeCode() m_isComputeUnitJarsSet : {}", m_isComputeUnitJarsSet);
		if (m_isComputeUnitJarsSet) {
			djson4 = addComputeUnitJars(djson4);
			m_isComputeUnitJarsSet = false;
			m_dcl = null;
		}

		//StringEntity params = new StringEntity(djson4.toString());
	    //postRequest.addHeader("content-type", "application/json");
	    //postRequest.setEntity(params);

	    postRequest.addHeader("Content-Encoding", "gzip");
	    postRequest.addHeader("Accept-Encoding", "gzip");

	    //=====================================================================
	    ByteArrayOutputStream bytestream = new ByteArrayOutputStream();
	    try (GZIPOutputStream gzipstream = new GZIPOutputStream(bytestream)) {
	        gzipstream.write(djson4.toString().getBytes("UTF-8"));
	    }
	    byte[] gzipBytes = bytestream.toByteArray();
	    bytestream.close();
	    ByteArrayEntity byteEntity = new ByteArrayEntity(gzipBytes);
	    postRequest.setEntity(byteEntity);
	    //======================================================================

		log.debug("executeCode() executing request to " + m_target + "/Compute/Execute/BshCode");
		incCountRequests();
		Future<HttpResponse> futureHttpResponse = m_httpClient.execute(m_target, postRequest, null);
		return futureHttpResponse;
		
	}

	public Future<HttpResponse> executeClass(Class<? extends DComputeUnit> cls) throws ClientProtocolException, IOException {
		HttpPost postRequest = new HttpPost("/Compute/Execute/Class");
        
    	Class<? extends DComputeUnit> p = cls;
   		
    	String className = p.getName();
    	log.debug("executeClass() className  : {}", className);
  		String classAsPath = className.replace('.', '/') + ".class";
    	log.debug("executeClass() classAsPath  : {}", classAsPath);

  		InputStream in = p.getClassLoader().getResourceAsStream(classAsPath);
  		byte b[] = DMUtil.toBytesFromInStreamForJavaFiles(in);
  		in.close();
  		//log.debug("executeClass() b[] as string : {}", b.toString());
  		String hexStr = DMUtil.toHexString(b);
  		//log.debug("executeClass() Hex string is : {}", hexStr);
  		
		DMJson djson1 = new DMJson("TotalComputeUnit", "" + m_TU);
		DMJson djson2 = djson1.add("SplitComputeUnit", "" + m_SU);
		DMJson djson3 = djson2.add("JsonInput", "" + m_jsonStrInput);
		DMJson djson4 = djson3.add("ClassName", className);
		DMJson djson5 = djson4.add("ClassBytes", hexStr);
		
		log.debug("executeClass() m_jarFilePathList.size() : {}", m_jarFilePathList.size());
		if (m_jarFilePathList.size() > 0) {
			djson5 = addJars(djson5);
			m_jarFilePathList.clear();
		}
		
		log.debug("executeClass() m_isComputeUnitJarsSet : {}", m_isComputeUnitJarsSet);
		if (m_isComputeUnitJarsSet) {
			djson5 = addComputeUnitJars(djson5);
			m_isComputeUnitJarsSet = false;
			m_dcl = null;
		}

		//StringEntity params = new StringEntity(djson5.toString());
	    //postRequest.addHeader("content-type", "application/json");
	    //postRequest.setEntity(params);
	    
	    postRequest.addHeader("Content-Encoding", "gzip");
	    postRequest.addHeader("Accept-Encoding", "gzip");

	    //=====================================================================
	    ByteArrayOutputStream bytestream = new ByteArrayOutputStream();
	    try (GZIPOutputStream gzipstream = new GZIPOutputStream(bytestream)) {
	        gzipstream.write(djson5.toString().getBytes("UTF-8"));
	    }
	    byte[] gzipBytes = bytestream.toByteArray();
	    bytestream.close();
	    ByteArrayEntity byteEntity = new ByteArrayEntity(gzipBytes);
	    postRequest.setEntity(byteEntity);
	    //======================================================================
        			
		log.debug("executeClass() executing request to " + m_target + "/Compute/Execute/Class");
		incCountRequests();
		Future<HttpResponse> futureHttpResponse = m_httpClient.execute(m_target, postRequest, null);
		return futureHttpResponse;
		
	}

	public Future<HttpResponse> executeObject(DComputeUnit obj) throws ClientProtocolException, IOException {
		HttpPost postRequest = new HttpPost("/Compute/Execute/ClassFromObject");
		
    	Class<? extends DComputeUnit> p = obj.getClass();
    	String className = p.getName();
    	log.debug("executeObject() className  : {}", className);
  		String classAsPath = className.replace('.', '/') + ".class";
    	log.debug("executeObject() classAsPath  : {}", classAsPath);

  		InputStream in = p.getClassLoader().getResourceAsStream(classAsPath);
  		byte b[] = DMUtil.toBytesFromInStreamForJavaFiles(in);
  		in.close();
  		//log.debug("executeObject() b[] as string : {}", b.toString());
  		String hexStr = DMUtil.toHexString(b);
  		//log.debug("executeObject() Hex string is : {}", hexStr);
  		
		DMJson djson1 = new DMJson("TotalComputeUnit", "" + m_TU);
		DMJson djson2 = djson1.add("SplitComputeUnit", "" + m_SU);
		DMJson djson3 = djson2.add("JsonInput", "" + m_jsonStrInput);
		DMJson djson4 = djson3.add("ClassName", className);
		DMJson djson5 = djson4.add("ClassBytes", hexStr);

		log.debug("executeObject() m_jarFilePathList.size() : {}", m_jarFilePathList.size());
		if (m_jarFilePathList.size() > 0) {
			djson5 = addJars(djson5);
			m_jarFilePathList.clear();
		}
		
		log.debug("executeObject() m_isComputeUnitJarsSet : {}", m_isComputeUnitJarsSet);
		if (m_isComputeUnitJarsSet) {
			djson5 = addComputeUnitJars(djson5);
			m_isComputeUnitJarsSet = false;
			m_dcl = null;
		}
		
		//works StringEntity params = new StringEntity(djson5.toString());
	    //works postRequest.addHeader("content-type", "application/json");
		//works postRequest.setEntity(params);
		
		postRequest.addHeader("Content-Encoding", "gzip");
	    postRequest.addHeader("Accept-Encoding", "gzip");
	    	            	
	    //=====================================================================
	    ByteArrayOutputStream bytestream = new ByteArrayOutputStream();
	    try (GZIPOutputStream gzipstream = new GZIPOutputStream(bytestream)) {
	        gzipstream.write(djson5.toString().getBytes("UTF-8"));
	    }
	    byte[] gzipBytes = bytestream.toByteArray();
	    bytestream.close();
	    ByteArrayEntity byteEntity = new ByteArrayEntity(gzipBytes);
	    postRequest.setEntity(byteEntity);
	    //======================================================================
	    
		log.debug("executeObject() executing request to " + m_target + "/Compute/Execute/ClassFromObject");
		incCountRequests();
		Future<HttpResponse> futureHttpResponse = m_httpClient.execute(m_target, postRequest, null);
		return futureHttpResponse;
		
	}
	
	public Future<HttpResponse> executeClassNameInJar(String jarFilePath, String classNameInJar) throws ClientProtocolException, IOException {
		HttpPost postRequest = new HttpPost("/Compute/Execute/ClassNameInJar");
        
  		InputStream in = new FileInputStream(jarFilePath);
  		byte b[] = DMUtil.toBytesFromInStreamForJavaFiles(in);
  		in.close();
  		//log.debug("executeClassNameInJar() b[] as string : {}", b.toString());
  		String hexStr = DMUtil.toHexString(b);
  		//log.debug("executeClassNameInJar() Hex string is : {}", hexStr);
  		
		DMJson djson1 = new DMJson("TotalComputeUnit", "" + m_TU);
		DMJson djson2 = djson1.add("SplitComputeUnit", "" + m_SU);
		DMJson djson3 = djson2.add("JsonInput", "" + m_jsonStrInput);
		DMJson djson4 = djson3.add("ClassNameInJar", classNameInJar);
		DMJson djson5 = djson4.add("JarFilePath", jarFilePath);
		DMJson djson6 = djson5.add("JarBytes", hexStr);

		log.debug("executeClassNameInJar() m_jarFilePathList.size() : {}", m_jarFilePathList.size());
		if (m_jarFilePathList.size() > 0) {
			djson6 = addJars(djson6);
			m_jarFilePathList.clear();
		}
		
		log.debug("executeClassNameInJar() m_isComputeUnitJarsSet : {}", m_isComputeUnitJarsSet);
		if (m_isComputeUnitJarsSet) {
			djson6 = addComputeUnitJars(djson6);
			m_isComputeUnitJarsSet = false;
			m_dcl = null;
		}

		//StringEntity params = new StringEntity(djson5.toString());
	    //postRequest.addHeader("content-type", "application/json");
	    //postRequest.setEntity(params);
	    
	    postRequest.addHeader("Content-Encoding", "gzip");
	    postRequest.addHeader("Accept-Encoding", "gzip");

	    //=====================================================================
	    ByteArrayOutputStream bytestream = new ByteArrayOutputStream();
	    try (GZIPOutputStream gzipstream = new GZIPOutputStream(bytestream)) {
	        gzipstream.write(djson6.toString().getBytes("UTF-8"));
	    }
	    byte[] gzipBytes = bytestream.toByteArray();
	    bytestream.close();
	    ByteArrayEntity byteEntity = new ByteArrayEntity(gzipBytes);
	    postRequest.setEntity(byteEntity);
	    //======================================================================
        			
		log.debug("executeClassNameInJar() executing request to " + m_target + "/Compute/Execute/ClassNameInJar");
		incCountRequests();
		Future<HttpResponse> futureHttpResponse = m_httpClient.execute(m_target, postRequest, null);
		return futureHttpResponse;
		
	}
	
	private DMJson addJars(DMJson dmjson) throws IOException {
		
		DMJson dmjsonout = null;
		int i = 1;
		
		for (String jarFilePath : m_jarFilePathList) {
			log.debug("addJars() jarFilePath : {}", jarFilePath);
	  		InputStream in = new FileInputStream(jarFilePath);
	  		byte b[] = DMUtil.toBytesFromInStreamForJavaFiles(in);
	  		in.close();
	  		//log.debug("addJars() b[] as string : {}", b.toString());
	  		String hexStr = DMUtil.toHexString(b);
	  		//log.debug("addJars() Hex string is : {}", hexStr);
	  		if (null == dmjsonout) {
	  			//dmjsonout = new DMJson("" + i, hexStr);
	  			dmjsonout = new DMJson(jarFilePath, hexStr);
	  		} else {
	  			//dmjsonout = dmjsonout.add("" + i, hexStr);
	  			dmjsonout = dmjsonout.add(jarFilePath, hexStr);
	  		}
	  		i++;
		}
		DMJson outJson = null;
		if (dmjsonout != null) {
			outJson = dmjson.add("AddJars", dmjsonout.toString());
			//log.debug("addJars() outJson.toString() : {}", outJson.toString());
		}
		return outJson;
	}
	
	public int addJar(String jarFilePath) throws DScabiException {
		if (null == jarFilePath)
			throw new DScabiException("jarFilePath is null", "CNB.AJR.1");
		m_jarFilePathList.add(jarFilePath);
		return 0;
		
	}
	
	public int setJarFilePathFromList(List<String> jarFilePathList) {
		m_jarFilePathList.addAll(jarFilePathList);
		return 0;
	}
	
	private DMJson addComputeUnitJars(DMJson dmjson) throws IOException {
		
		DMJson dmjsonout = null;
	
		HashMap<String, byte[]> map = m_dcl.getMapJarFilePathJarBytes();
		Set<String> st = map.keySet();
		for (String jarFilePath : st) {
			log.debug("addComputeUnitJars() jarFilePath : {}", jarFilePath);
	  		
	  		byte b[] = map.get(jarFilePath);
	  		
	  		//log.debug("addComputeUnitJars() b[] as string : {}", b.toString());
	  		String hexStr = DMUtil.toHexString(b);
	  		//log.debug("addComputeUnitJars() Hex string is : {}", hexStr);
	  		if (null == dmjsonout) {
	  			dmjsonout = new DMJson(jarFilePath, hexStr);
	  		} else {
	  			dmjsonout = dmjsonout.add(jarFilePath, hexStr);
	  		}
	  		
		}
		DMJson outJson = null;
		if (dmjsonout != null) {
			
			if (false == dmjson.contains("AddJars")) {
				outJson = dmjson.add("AddJars", dmjsonout.toString());
			} else {
				String existingAddJarsStr = dmjson.getString("AddJars");
				DMJson dmjsonnew = new DMJson(existingAddJarsStr) ;
				Set<String> st2 = dmjsonout.keySet();
				for (String s2 : st2) {
					dmjsonnew = dmjsonnew.add(s2, dmjsonout.getString(s2));
				}
				
				Set<String> st3 = dmjson.keySet();
				for (String s3 : st3) {
					if (s3.equalsIgnoreCase("AddJars"))
						continue;
					if (null == outJson)
						outJson = new DMJson(s3, dmjson.getString(s3));
					else
						outJson = outJson.add(s3, dmjson.getString(s3));
				}
				
				outJson = outJson.add("AddJars", dmjsonnew.toString());
			
			}
			log.debug("addComputeUnitJars() outJson.toString() : {}", outJson.toString());
					
		}
		return outJson;
	}

}

	



