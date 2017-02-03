/**
 * @author Dilshad Mustafa
 * Copyright (c) Dilshad Mustafa
 * All Rights Reserved.
 * Created 18-Jul-2016
 * File Name : DataNoBlock.java
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

package com.dilmus.dilshad.scabi.core.data;

import java.io.IOException;

import org.apache.http.client.ClientProtocolException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
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
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
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
import com.dilmus.dilshad.scabi.core.DataUnit;
import com.dilmus.dilshad.scabi.core.compute.DComputeAsyncConfig;
import com.dilmus.dilshad.scabi.deprecated.DOperatorConfig_2_1;
import com.dilmus.dilshad.scabi.deprecated.DOperatorConfig_2_2;

/**
 * @author Dilshad Mustafa
 *
 */
public class DataNoBlock {

	private static final Logger log = LoggerFactory.getLogger(DataNoBlock.class);
	private static CloseableHttpAsyncClient m_httpClient = null;
	private String m_jsonString = null;
	private HttpHost m_target = null;
	private String m_computeHost = null;
	private String m_computePort = null;
	private DMJson m_djson = null;
	private String m_jsonStrInput = null;
	private HttpHost m_metaTarget = null;
	private DMeta m_meta = null;
	private long m_TU = 0;
	private long m_SU = 0;
	private boolean m_isFaulty = false;
	
	private int MAX_REQUESTS = 1000;
	private int m_countRequests = 0;
	private Object m_lockCountRequests = new Object();
	
	private LinkedList<String> m_jarFilePathList = null;
	
	private boolean m_isComputeUnitJarsSet = false;
	private DMClassLoader m_dcl = null;
	
	private String m_jobId = null;
	private String m_configId = null;
	private String m_taskId = null;
	
	private long m_startCommandId = -1;
	private long m_endCommandId = -1;
	
	private DMLimiter m_clientPortLimiter = null;
	private long m_clientConnectionsPerRoute = 0;
	private long m_maxClientConnectionsPerRoute = Integer.MAX_VALUE; // get from HttpAsyncClient;
	private static boolean m_isStarted = false;
	
	private IConfig m_config = null;
	
	private String m_appName = null;
	private String m_appId = null;
	
	private long m_retryNumber = -1;
	private long m_maxRetry = -1;
	
	static {
		// Previous works m_httpClient = HttpAsyncClients.createDefault();

        HttpAsyncClientBuilder b = HttpAsyncClients.custom().setMaxConnTotal(Integer.MAX_VALUE)
															.setMaxConnPerRoute(Integer.MAX_VALUE);
        m_httpClient = b.build();
	
	}

	public int setRetryNumber(long retryNumber) {
		m_retryNumber = retryNumber;
		return 0;
	}
	
	public int setMaxRetry(long maxRetry) {
		m_maxRetry = maxRetry;
		return 0;
	}
	
	public int setAppName(String appName) {
		m_appName = appName;
		return 0;
	}
	
	public String getAppName() {
		return m_appName;
	}
	
	public int setAppId(String appId) {
		m_appId = appId;
		return 0;
	}
	
	public String getAppId() {
		return m_appId;
	}	
	
	public int setConfig(IConfig config) {
		m_config = config;
		
		return 0;
	}
	
	public int setClientPortLimiter(DMLimiter clientPortLimiter) {
		m_clientPortLimiter = clientPortLimiter;
		return 0;
	}
	
	public boolean isClientPortAllowed() {
		return m_clientPortLimiter.isAllowed();
	}
	
	public int decClientPortCount() {
		
		if (m_clientConnectionsPerRoute > 0) {
			m_clientConnectionsPerRoute--;
			m_clientPortLimiter.dec();
		}

		return 0;
	}
	
	public int incClientPortCount() {
		
		if (m_clientConnectionsPerRoute < m_maxClientConnectionsPerRoute) {
			m_clientConnectionsPerRoute++;
			m_clientPortLimiter.inc();
		}

		return 0;
	}
	
	public int setCommandIdRange(long startCommandId, long endCommandId) {
		m_startCommandId = startCommandId;
		m_endCommandId = endCommandId;
		
		return 0;
	}
	
	public int clearCommandIdRange() {
		m_startCommandId = -1;
		m_endCommandId = -1;
		return 0;
	}
	
	public int setJobId(String jobId) {
		m_jobId = jobId;
		return 0;
	}
	
	public String getJobId() {
		return m_jobId;
	}
	
	public int setConfigId(String configId) {
		m_configId = configId;
		return 0;
	}
	
	public String getConfigId() {
		return m_configId;
	}
	
	public int setTaskId(String taskId) {
		m_taskId = taskId;
		return 0;
	}
	
	public String getTaskId() {
		return m_taskId;
	}
		
	public int setComputeUnitJars(DMClassLoader dcl) {
		m_isComputeUnitJarsSet = true;
		m_dcl = dcl;
		return 0;
	}
	
	public DataNoBlock(String jsonString) throws IOException {

		m_djson = new DMJson(jsonString);
		m_computeHost = m_djson.getString("ComputeHost");
		m_computePort = m_djson.getString("ComputePort");

		try {
			// Previous works m_httpClient = HttpAsyncClients.createDefault();
	        // Previous works m_httpClient.start();
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
		
		m_jarFilePathList = new LinkedList<String>();
		
		m_jobId = DMJson.empty();
		m_configId = DMJson.empty();
		m_taskId = DMJson.empty();
		
		m_startCommandId = -1;
		m_endCommandId = -1;
		
		m_appName = DMJson.empty();
		m_appId = DMJson.empty();
	}
	
	public DataNoBlock(DMeta meta) throws Exception {
		
		String jsonCompute = null;
		try {
			// Previous works m_httpClient = HttpAsyncClients.createDefault();
			// Previous works m_httpClient.start();
			Future<HttpResponse> futureHttpResponse = computeAlloc(meta);
			HttpResponse httpResponse = DataNoBlock.get(futureHttpResponse);
			jsonCompute = DataNoBlock.getResult(httpResponse);
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
		
		m_jarFilePathList = new LinkedList<String>(); // new ArrayList<String>();
		
		m_jobId = DMJson.empty();
		m_configId = DMJson.empty();
		m_taskId = DMJson.empty();
		
		m_startCommandId = -1;
		m_endCommandId = -1;
		
		m_appName = DMJson.empty();
		m_appId = DMJson.empty();
	}
	
	public int close() throws IOException {
		/* Previous works
		if (null != m_httpClient) {
			m_httpClient.close();
			m_httpClient = null;
		}	
		*/
		
		return 0;
	}
	
	public static int startHttpAsyncService() {
		synchronized(m_httpClient) {
			if (false == m_isStarted) {
				m_httpClient.start();
				m_isStarted = true;
			}
		}
		return 0;
	}

	public static int closeHttpAsyncService() throws IOException {
		synchronized(m_httpClient) {
			if (m_isStarted) {
				m_httpClient.close();
				m_isStarted = false;
			}
		}
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
			if (null == jsonString)
				return DMJson.error("CNB.GRT.1", "Response is null. Full response is : " + entity.toString());
			if (0 == jsonString.length())
				return DMJson.error("CNB.GRT.2", "Response is empty. Full response is : " + entity.toString());
		} else {
			return DMJson.error("CNB.GRT.3", "Entity is null");
		}
		
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

	public int setTU(long tu) {
		m_TU = tu;
		return 0;
	}

	public int setSU(long su) {
		m_SU = su;
		return 0;
	}
	
	public long getTU() {
		return m_TU;
	}

	public long getSU() {
		return m_SU;
	}

	public Future<HttpResponse> isDoneProcessing() throws ClientProtocolException, IOException, DScabiException {

		if (0 == m_TU) {
			log.debug("isDoneProcessing() m_TU is not set. m_SU : {}", m_SU);
			throw new RuntimeException(new DScabiException("m_TU is not set. m_SU : " + m_SU, "DNB.IDP.1"));			
		}
		if (0 == m_SU) {
			log.debug("isDoneProcessing() m_SU is not set. m_SU : {}", m_SU);
			throw new RuntimeException(new DScabiException("m_SU is not set. m_SU : " + m_SU, "DNB.IDP.1"));			
		}
		
		HttpPost postRequest = new HttpPost("/Task/IsDoneProcessing");

		DMJson djson1 = new DMJson("TotalComputeUnit", "" + m_TU);
		DMJson djson2 = djson1.add("SplitComputeUnit", "" + m_SU);
		djson2.add("AppName", m_appName);
		djson2.add("AppId", m_appId);
		djson2.add("JobId", m_jobId);
		djson2.add("ConfigId", m_configId);
		djson2.add("TaskId", m_taskId);

		//StringEntity params = new StringEntity(djson2.toString());
	    //postRequest.addHeader("content-type", "application/json");
	    //postRequest.setEntity(params);

	    postRequest.addHeader("Content-Encoding", "gzip");
	    postRequest.addHeader("Accept-Encoding", "gzip");

	    //=====================================================================
	    ByteArrayOutputStream bytestream = new ByteArrayOutputStream();
	    try (GZIPOutputStream gzipstream = new GZIPOutputStream(bytestream)) {
	        gzipstream.write(djson2.toString().getBytes("UTF-8"));
	    }
	    byte[] gzipBytes = bytestream.toByteArray();
	    bytestream.close();
	    ByteArrayEntity byteEntity = new ByteArrayEntity(gzipBytes);
	    postRequest.setEntity(byteEntity);
	    //======================================================================

		log.debug("isDoneProcessing() executing request to " + m_target + "/Task/IsDoneProcessing");

		Future<HttpResponse> futureHttpResponse = m_httpClient.execute(m_target, postRequest, null);

		m_TU = 0;
		m_SU = 0;
		
		return futureHttpResponse;
		
	}	
	
	public Future<HttpResponse> retrieveResult() throws ClientProtocolException, IOException, DScabiException {

		if (0 == m_TU) {
			log.debug("retrieveResult() m_TU is not set. m_SU : {}", m_SU);
			throw new RuntimeException(new DScabiException("m_TU is not set. m_SU : " + m_SU, "DNB.RRT.1"));			
		}
		if (0 == m_SU) {
			log.debug("retrieveResult() m_SU is not set. m_SU : {}", m_SU);
			throw new RuntimeException(new DScabiException("m_SU is not set. m_SU : " + m_SU, "DNB.RRT.1"));			
		}
		
		HttpPost postRequest = new HttpPost("/Task/RetrieveResult");

		DMJson djson1 = new DMJson("TotalComputeUnit", "" + m_TU);
		DMJson djson2 = djson1.add("SplitComputeUnit", "" + m_SU);
		djson2.add("AppName", m_appName);
		djson2.add("AppId", m_appId);
		djson2.add("JobId", m_jobId);
		djson2.add("ConfigId", m_configId);
		djson2.add("TaskId", m_taskId);

		//StringEntity params = new StringEntity(djson2.toString());
	    //postRequest.addHeader("content-type", "application/json");
	    //postRequest.setEntity(params);

	    postRequest.addHeader("Content-Encoding", "gzip");
	    postRequest.addHeader("Accept-Encoding", "gzip");

	    //=====================================================================
	    ByteArrayOutputStream bytestream = new ByteArrayOutputStream();
	    try (GZIPOutputStream gzipstream = new GZIPOutputStream(bytestream)) {
	        gzipstream.write(djson2.toString().getBytes("UTF-8"));
	    }
	    byte[] gzipBytes = bytestream.toByteArray();
	    bytestream.close();
	    ByteArrayEntity byteEntity = new ByteArrayEntity(gzipBytes);
	    postRequest.setEntity(byteEntity);
	    //======================================================================

		log.debug("retrieveResult() executing request to " + m_target + "/Task/RetrieveResult");

		Future<HttpResponse> futureHttpResponse = m_httpClient.execute(m_target, postRequest, null);
		
		m_TU = 0;
		m_SU = 0;
		
		return futureHttpResponse;
		
	}		
	
	public Future<HttpResponse> executeForDataUnitOperators(HashMap<String, DataAsyncConfigNode> commandMap) throws ClientProtocolException, IOException, DScabiException {
		
		if (0 == m_TU) {
			log.debug("executeForDataUnitOperators() m_TU is not set. m_SU : {}", m_SU);
			throw new RuntimeException(new DScabiException("m_TU is not set. m_SU : " + m_SU, "DNB.EFDO.1"));			
		}
		if (0 == m_SU) {
			log.debug("executeForDataUnitOperators() m_SU is not set. m_SU : {}", m_SU);
			throw new RuntimeException(new DScabiException("m_SU is not set. m_SU : " + m_SU, "DNB.EFDO.1"));			
		}
		if (m_startCommandId <= 0) {
			log.debug("executeForDataUnit() m_startCommandId is not set. m_startCommandId : {}", m_startCommandId);
			throw new DScabiException("m_startCommandId is not set. m_startCommandId : " + m_startCommandId, "DNB.EFDO.1");			
		}
		if (m_endCommandId <= 0) {
			log.debug("executeForDataUnit() m_endCommandId is not set. m_endCommandId : {}", m_endCommandId);
			throw new DScabiException("m_endCommandId is not set. m_endCommandId : " + m_endCommandId, "DNB.EFDO.1");			
		}
		if (m_startCommandId > m_endCommandId) {
			log.debug("executeForDataUnit() m_startCommandId is greater than m_endCommandId. m_startCommandId : {}, m_endCommandId : {}", m_startCommandId, m_endCommandId);
			throw new DScabiException("m_startCommandId is greater than m_endCommandId. m_startCommandId : " + m_startCommandId + " m_endCommandId : " + m_endCommandId, "DNB.EFDO.1");			
		}
		if (-1 == m_retryNumber) {
			log.debug("executeForDataUnit() m_retryNumber is not set. m_retryNumber : {}", m_retryNumber);
			throw new DScabiException("m_retryNumber is not set. m_retryNumber : " + m_retryNumber, "DNB.EFDO.1");			
		}
		if (-1 == m_maxRetry) {
			log.debug("executeForDataUnit() m_maxRetry is not set. m_maxRetry : {}", m_maxRetry);
			throw new DScabiException("m_maxRetry is not set. m_maxRetry : " + m_maxRetry, "DNB.EFDO.1");			
		}
		
		log.debug("executeForDataUnit() m_startCommandId : {}", m_startCommandId);
		log.debug("executeForDataUnit() m_endCommandId : {}", m_endCommandId);
		
		HttpPost postRequest = new HttpPost("/Data/Execute/ExecuteForDataUnit");
         		
		DMJson djson1 = new DMJson("TotalComputeUnit", "" + m_TU);
		djson1.add("SplitComputeUnit", "" + m_SU);
		djson1.add("AppName", m_appName);
		djson1.add("AppId", m_appId);
		djson1.add("JobId", m_jobId);
		djson1.add("ConfigId", m_configId);
		djson1.add("TaskId", m_taskId);
		djson1.add("StartCommandId", "" + m_startCommandId);
		djson1.add("EndCommandId", "" + m_endCommandId);
		djson1.add("RetryNumber", "" + m_retryNumber);
		djson1.add("MaxRetry", "" + m_maxRetry);
		
		long n = 1;
		// Not needed Set<String> st = commandMap.keySet();
		
		for (long i = m_startCommandId; i <= m_endCommandId; i++) {
			
			Class<?> p = null;
			DMJson d = new DMJson();
			String hexStr = null;
			
			DataAsyncConfigNode configNode = commandMap.get("" + i);
			d.add("CommandId", "" + i);
			d.add("ConfigNodeType", "" + configNode.getConfigNodeType());
			
			if (DataAsyncConfigNode.CNT_DATAUNIT_CONFIG == configNode.getConfigNodeType()) {
				DataUnitConfig dataUnitConfig = configNode.getDataUnitConfig();
				d.add("ConfigType", dataUnitConfig.getConfigType());
				d.add("DataId", dataUnitConfig.getDataId());
				d.add("JsonInput", dataUnitConfig.getInput());
				if (DataUnitConfig.CFG_TYPE_CLASS == dataUnitConfig.getConfigType()) {
					p = dataUnitConfig.getDataUnitClass();
				} else if (DataUnitConfig.CFG_TYPE_OBJECT == dataUnitConfig.getConfigType()) {
					p = dataUnitConfig.getDataUnitObject().getClass();
				}
				hexStr = dataUnitConfig.getJavaFileAsHexStr();
			} else if (DataAsyncConfigNode.CNT_PARTITIONER_CONFIG == configNode.getConfigNodeType()) {
				DMPartitionerConfig partitionerConfig = configNode.getPartitionerConfig();
				d.add("ConfigType", partitionerConfig.getConfigType());
				d.add("DataId", partitionerConfig.getDataId());
				d.add("JsonInput", partitionerConfig.getInput());
				if (DMPartitionerConfig.CFG_TYPE_CLASS == partitionerConfig.getConfigType()) {
					p = partitionerConfig.getPartitionerClass();
				} else if (DMPartitionerConfig.CFG_TYPE_OBJECT == partitionerConfig.getConfigType()) {
					p = partitionerConfig.getPartitionerObject().getClass();
				}
				hexStr = partitionerConfig.getJavaFileAsHexStr();
			} else if (DataAsyncConfigNode.CNT_OPERATOR_CONFIG_1_1 == configNode.getConfigNodeType()) {
				DMOperatorConfig_1_1 operatorConfig_1_1 = configNode.getOperatorConfig_1_1();
				d.add("ConfigType", operatorConfig_1_1.getConfigType());
				d.add("SourceDataId", operatorConfig_1_1.getSourceDataId());
				d.add("TargetDataId", operatorConfig_1_1.getTargetDataId());
				d.add("JsonInput", operatorConfig_1_1.getInput());
				if (DMOperatorConfig_1_1.CFG_TYPE_CLASS_OF_INTERFACE == operatorConfig_1_1.getConfigType()) {
					p = operatorConfig_1_1.getOperatorClassOfInterface();
				} else if (DMOperatorConfig_1_1.CFG_TYPE_OBJECT_OF_INTERFACE == operatorConfig_1_1.getConfigType()) {
					p = operatorConfig_1_1.getOperatorObjectOfInterface().getClass();
				}
				hexStr = operatorConfig_1_1.getJavaFileAsHexStr();
			} else if (DataAsyncConfigNode.CNT_OPERATOR_CONFIG_1_2 == configNode.getConfigNodeType()) {
				DMOperatorConfig_1_2 operatorConfig_1_2 = configNode.getOperatorConfig_1_2();
				d.add("ConfigType", operatorConfig_1_2.getConfigType());
				d.add("SourceDataId", operatorConfig_1_2.getSourceDataId());
				d.add("TargetDataId", operatorConfig_1_2.getTargetDataId());
				d.add("LambdaMethodName", operatorConfig_1_2.getLambdaMethodName());
				d.add("JsonInput", operatorConfig_1_2.getInput());
				if (DMOperatorConfig_1_2.CFG_TYPE_CLASS == operatorConfig_1_2.getConfigType()) {
					p = operatorConfig_1_2.getOperatorClass();
				} 
				/* Not used
				else if (DMOperatorConfig_1_2.CFG_TYPE_CLASS_OF_INTERFACE == operatorConfig_1_2.getConfigType()) {
					p = operatorConfig_1_2.getOperatorClassOfInterface();
				} else if (DMOperatorConfig_1_2.CFG_TYPE_OBJECT_OF_INTERFACE == operatorConfig_1_2.getConfigType()) {
					p = operatorConfig_1_2.getOperatorObjectOfInterface().getClass();
				}
				*/
				hexStr = operatorConfig_1_2.getJavaFileAsHexStr();
			} else if (DataAsyncConfigNode.CNT_SHUFFLE_CONFIG_1_1 == configNode.getConfigNodeType()) {
				DMShuffleConfig_1_1 shuffleConfig_1_1 = configNode.getShuffleConfig_1_1();
				d.add("ConfigType", shuffleConfig_1_1.getConfigType());
				d.add("SourceDataId", shuffleConfig_1_1.getSourceDataId());
				d.add("TargetDataId", shuffleConfig_1_1.getTargetDataId());
				d.add("JsonInput", shuffleConfig_1_1.getInput());
				if (DMShuffleConfig_1_1.CFG_TYPE_CLASS_OF_INTERFACE == shuffleConfig_1_1.getConfigType()) {
					p = shuffleConfig_1_1.getShuffleClassOfInterface();
				} else if (DMShuffleConfig_1_1.CFG_TYPE_OBJECT_OF_INTERFACE == shuffleConfig_1_1.getConfigType()) {
					p = shuffleConfig_1_1.getShuffleObjectOfInterface().getClass();
				}
				hexStr = shuffleConfig_1_1.getJavaFileAsHexStr();
			} else if (DataAsyncConfigNode.CNT_SHUFFLE_CONFIG_1_2 == configNode.getConfigNodeType()) {
				DMShuffleConfig_1_2 shuffleConfig_1_2 = configNode.getShuffleConfig_1_2();
				d.add("ConfigType", shuffleConfig_1_2.getConfigType());
				d.add("SourceDataId", shuffleConfig_1_2.getSourceDataId());
				d.add("TargetDataId", shuffleConfig_1_2.getTargetDataId());
				d.add("LambdaMethodName", shuffleConfig_1_2.getLambdaMethodName());
				d.add("JsonInput", shuffleConfig_1_2.getInput());
				if (DMShuffleConfig_1_2.CFG_TYPE_CLASS == shuffleConfig_1_2.getConfigType()) {
					p = shuffleConfig_1_2.getShuffleClass();
				} 
				hexStr = shuffleConfig_1_2.getJavaFileAsHexStr();
			} else {
				throw new DScabiException("Invalid config node type", "DNB.EFD.1");
			}

	    	String className = p.getName();
	    	log.debug("executeForDataUnit() className  : {}", className);
	  		String classAsPath = className.replace('.', '/') + ".class";
	    	log.debug("executeForDataUnit() classAsPath  : {}", classAsPath);

	    	/* Previous works
	  		InputStream in = p.getClassLoader().getResourceAsStream(classAsPath);
	  		byte b[] = DMUtil.toBytesFromInStreamForJavaFiles(in);
	  		in.close();
	  		//log.debug("executeForDataUnit() b[] as string : {}", b.toString());
	  		String hexStr = DMUtil.toHexString(b);
	  		//log.debug("executeForDataUnit() Hex string is : {}", hexStr);
	 		*/
	    	
			d.add("ClassName", className);
			d.add("ClassBytes", hexStr);	
			log.debug("executeForDataUnit() d is : {}", d.toString());
			djson1.add("" + n, d.toString());
			n++;
		}
		djson1.add("ConfigCount", "" + (n - 1));
		log.debug("executeForDataUnit() djson1 is : {}", djson1.toString());
		
		/* Previous works
		log.debug("executeForDataUnit() m_jarFilePathList.size() : {}", m_jarFilePathList.size());
		if (m_jarFilePathList.size() > 0) {
			djson1 = addJars(djson1);
			m_jarFilePathList.clear();
		}
		*/
		
		/* New Previous works
		log.debug("executeClass() m_config.isJarFilePathListSet() : {}", m_config.isJarFilePathListSet());
		if (m_config.isJarFilePathListSet()) {
			djson1.add("AddJars", m_config.getAllJarFilesHexStrJsonStr());
		}
		*/
		
		/* Previous works
		log.debug("executeForDataUnit() m_isComputeUnitJarsSet : {}", m_isComputeUnitJarsSet);
		if (m_isComputeUnitJarsSet) {
			djson1 = addComputeUnitJars(djson1);
			m_isComputeUnitJarsSet = false;
			m_dcl = null;
		}
		*/
		
		/* New Previous works
		log.debug("executeClass() m_config.isComputeUnitJarsSet() : {}", m_config.isComputeUnitJarsSet());
		if (m_config.isComputeUnitJarsSet()) {
			djson1 = addComputeUnitJarsFromConfig(djson1);
		}
		*/
		
		log.debug("executeForDataUnit() m_config.isCombinedJarsSet() : {}", m_config.isCombinedJarsSet());
		if (m_config.isCombinedJarsSet()) {
			if (m_config.getCombinedJarFilesHexStrJsonStr() != null) {
				djson1.add("AddJars", m_config.getCombinedJarFilesHexStrJsonStr());
			}
		} else
			throw new DScabiException("Combined Jars is not set", "DNB.EDT.1");
		
		//StringEntity params = new StringEntity(djson5.toString());
	    //postRequest.addHeader("content-type", "application/json");
	    //postRequest.setEntity(params);
	    
	    postRequest.addHeader("Content-Encoding", "gzip");
	    postRequest.addHeader("Accept-Encoding", "gzip");

	    //=====================================================================
	    ByteArrayOutputStream bytestream = new ByteArrayOutputStream();
	    try (GZIPOutputStream gzipstream = new GZIPOutputStream(bytestream)) {
	        gzipstream.write(djson1.toString().getBytes("UTF-8"));
	    }
	    byte[] gzipBytes = bytestream.toByteArray();
	    bytestream.close();
	    ByteArrayEntity byteEntity = new ByteArrayEntity(gzipBytes);
	    postRequest.setEntity(byteEntity);
	    //======================================================================
        			
		log.debug("executeForDataUnit() executing request to " + m_target + "/Compute/Execute/Class");

		Future<HttpResponse> futureHttpResponse = m_httpClient.execute(m_target, postRequest, null);
		
		clearCommandIdRange();
		m_TU = 0;
		m_SU = 0;
		m_retryNumber = -1;
		m_maxRetry = -1;
		
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

	private DMJson addComputeUnitJarsFromConfig(DMJson dmjson) throws IOException {
		
		DMJson dmjsonout = null;
		// Don't modify this object
		dmjsonout = m_config.getAllComputeUnitJarFilesHexStrDMJson();
		DMJson outJson = null;
		if (dmjsonout != null) {
			
			if (false == dmjson.contains("AddJars")) {
				outJson = dmjson.add("AddJars", dmjsonout.toString());
			} else {
				// Create new, don't modify original DMJson object
				DMJson dmjsonnew = new DMJson(m_config.getAllJarFilesHexStrJsonStr());
				Set<String> st2 = dmjsonout.keySet();
				for (String s2 : st2) {
					dmjsonnew = dmjsonnew.add(s2, dmjsonout.getString(s2));
				}
				
				/* Previous works
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
				*/
				dmjson.remove("AddJars");
				outJson = dmjson.add("AddJars", dmjsonnew.toString());
			
			}
			log.debug("addComputeUnitJarsFromConfig() outJson.toString() : {}", outJson.toString());
					
		}
		return outJson;
	}
	
}

	



