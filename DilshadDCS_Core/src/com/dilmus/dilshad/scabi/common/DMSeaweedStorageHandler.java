package com.dilmus.dilshad.scabi.common;
/**
 * @author Dilshad Mustafa
 * Copyright (c) Dilshad Mustafa
 * All Rights Reserved.
 * Created 18-Jul-2016
 * File Name : DMSeaweedStorageHandler.java
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


import java.io.IOException;

import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dilmus.dilshad.storage.IStorageHandler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.nio.entity.NByteArrayEntity;
import org.apache.http.nio.protocol.HttpAsyncRequestConsumer;
import org.apache.http.util.Asserts;
import org.apache.http.util.EntityUtils;
import org.apache.http.entity.mime.MultipartEntityBuilder;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
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
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// Not used import javax.json.JsonObject;

/**
 * @author Dilshad Mustafa
 *
 */
public class DMSeaweedStorageHandler implements IStorageHandler {

	private static final Logger log = LoggerFactory.getLogger(DMSeaweedStorageHandler.class);
	private static CloseableHttpAsyncClient m_httpClient = null;
	private HttpHost m_target = null;
	private String m_computeHost = null;
	private String m_computePort = null;
	private HttpHost m_metaTarget = null;
	
	private int MAX_REQUESTS = 1000;
	private int m_countRequests = 0;
	private Object m_lockCountRequests = new Object();
	
	private long m_clientConnectionsPerRoute = 0;
	private long m_maxClientConnectionsPerRoute = Integer.MAX_VALUE; // get from HttpAsyncClient;
	private static boolean m_isStarted = false;
	
	private int INTERNAL_BUFFER_SIZE = 1024; // 64 * 1024 * 1024
	private static final DMCounter m_gcCounter = new DMCounter();
	
	private HashMap<String, String> m_hostMap = new HashMap<String, String>();
	private HashMap<String, String> m_portMap = new HashMap<String, String>();
	private int m_countHosts = 0;
	private HashMap<String, HttpHost> m_targetMap = new HashMap<String, HttpHost>();
	private DMCounter m_rr = new DMCounter();
	private static final DMCounter m_referenceCount = new DMCounter();
	
	/* Caution : static Semaphore(1 or 2)
	 * 
	 * In testing setup all processes are run within the same 4GB RAM Laptop. If Seaweed master and volume
	 * server are run in separate dedicated server hardware, then this number can be tweaked based on 
	 * how many concurrent requests can be handled by each Seaweed volume server and master
	 * 
	 * Recommended setup:-
	 * We can have each Compute Service running in a separate System with its own ./weed filer
	 * All ./weed filer in all systems connected to same <ip>:<port> of ./weed master
	 * All ./weed filer in all systems connected to same <ip>:<port> of Cassandra server
	 * 
	 * This code is tested and working fine in the following setup:-
	 * One 4GB RAM Laptop, Driver code Test_Data.java, Meta service and two Compute Service running in the same Laptop, 
	 * one ./weed master, one ./weed volume and one ./weed filer, one MongoDB instance, all are running in the 
	 * same Laptop
	 * 
	 * Seaweed commands used in testing:-
	 * 
	 * ./weed master -volumeSizeLimitMB=350 or for seaweedfs v0.70, ./weed master -idleTimeout=1000000 -volumeSizeLimitMB=350
	 * ./weed volume -idleTimeout=1000000 -max=10000 -mserver="localhost:9333" -dir="/home/<user>/mystorage/mystorage1"
	 * ./weed filer -dir="/home/<user>/mystorage/forfiler"
	 */
	private static Semaphore m_semaphore = new Semaphore(2); // 100 works for seaweedfs 0.74, cw 2 for seaweedfs v0.70
	
	private CloseableHttpClient m_syncClient = null;
	
	private int m_maxRetry = 100; // 5;
	
	static {
        // works for async HttpAsyncClientBuilder b = HttpAsyncClients.custom().setMaxConnTotal(Integer.MAX_VALUE)
        	// works for async 												.setMaxConnPerRoute(Integer.MAX_VALUE);
        // works for async m_httpClient = b.build();
	}

	private static void gc() {
		m_gcCounter.inc();
		
		/* Caution: 4 is chosen because there are 64MB of data involved in methods copyFromLocal(), copyIfExistsToLocal()
		 * In case of HttpAsyncClient, 64MB byte arrays for example mEntity.writeTo(baos2), baos2.toByteArray() are creating 64MB arrays 
		 * and entity EntityUtils.toByteArray(entity) is creating 64MB array
		 * In case of HttpClient, multiPartEntity.addBinaryBody() with new File() is reading from 64MB file and entity
		 * EntityUtils.toByteArray(entity) is creating 64MB array
		 * 
		 * now this method is called from executeAndGet(HttpGet request) and executeAndGet(HttpPost request)
		 * as these methods are called wherever 64MB arrays are involved
		 */
		
		if (m_gcCounter.value() >= 4) { 
			System.gc();
			m_gcCounter.set(0);
		}
	}
	
	public DMSeaweedStorageHandler(String delimitedConfig) throws IOException {
		
		// works for async startHttpAsyncService();
		
		String[] sa = delimitedConfig.split(";");
		int n = 1;
		
		for (int i = 0; i < sa.length; i++) {
			String[] sa2 = sa[i].split("-");
			
			String host = sa2[0];
			String port = sa2[1];
			
			m_hostMap.put("" + n, host);
			m_portMap.put("" + n, port);
			
			HttpHost target = new HttpHost(host, Integer.parseInt(port), "http");
			m_targetMap.put("" + n, target);
	        
			n++;
		}
		
		m_countHosts = n - 1;
		// System.out.println("DMSeaweedStorageHandler(String delimitedConfig) m_countHosts : " + m_countHosts);
		log.debug("DMSeaweedStorageHandler(String delimitedConfig) m_countHosts : {}", m_countHosts);		
		
		HttpClientBuilder client = HttpClientBuilder.create();
		client.setMaxConnPerRoute(Integer.MAX_VALUE);
		client.setMaxConnTotal(Integer.MAX_VALUE);
		client.setConnectionTimeToLive(Integer.MAX_VALUE, TimeUnit.SECONDS);
		
		m_syncClient = client.build();

	}
	
	/* Reference
	public DMSeaweedStorageHandler(String jsonStr) throws IOException {
		
		startHttpAsyncService();
		
		DMJson djson1 = new DMJson(jsonStr);
		Iterator<String> itr = djson1.fieldNames();
		int n = 1;
		
		while (itr.hasNext()) {
			String s = itr.next();
			String s2 = djson1.getString(s);
			DMJson djson2 = new DMJson(s2);
			
			String host = djson2.getString("Host");
			String port = djson2.getString("Port");
			
			m_hostMap.put("" + n, host);
			m_portMap.put("" + n, port);
			
			HttpHost target = new HttpHost(host, Integer.parseInt(port), "http");
			m_targetMap.put("" + n, target);
	        
			n++;
		}
		
		m_countHosts = n - 1;
		// System.out.println("DMSeaweedStorageHandler(String jsonStr) m_countHosts : " + m_countHosts);
		log.debug("DMSeaweedStorageHandler(String jsonStr) m_countHosts : {}", m_countHosts);		
		
		// Not used m_syncClient = HttpClientBuilder.create().build();
	}
	*/
	
	/* Reference
	public DMSeaweedStorageHandler() throws IOException {

		startHttpAsyncService();
		
		m_computeHost = "localhost";
		m_computePort = "8888";

		try {
	        m_target = new HttpHost(m_computeHost, Integer.parseInt(m_computePort), "http");
	        
			m_hostMap.put("1", m_computeHost);
			m_portMap.put("1", m_computePort);
			m_targetMap.put("1", m_target);
			m_countHosts = 1;
			
			// Not used m_syncClient = HttpClientBuilder.create().build();
			
    	} catch (Exception e) {
			//e.printStackTrace();
    		if (null != m_httpClient) 
    			m_httpClient.close();
    		throw e;
    	}

	}
	*/
	
	/* Method : copyFromLocal(String storageFilePath, String localFilePath)
	 * Assumptions for Storage system that does not support creation of directory given directory name
	 * Parameter : storageFilePath
	 * <AnyDummyStringWithoutSlash>/<ArrayFolder>/<PageFolder>/<PageFileName>
	 * <AnyDummyStringWithoutSlash> = <AppId> or "" (empty string) or any dummy string without File.separator ("/" or "\")
	 * 
	 * Construct file name from storageFilePath
	 * File name = <ArrayFolder>_<PageFolder>_<PageFileName> 
	 * example <ArrayFolder> = "mydata_1" <PageFolder> = "meta_data" <PageFileName> = "page-0.dat"
	 * file name = "mydata_1_meta_data_page-0.dat"
	 * 
	 * If the file already exist in the Storage System, overwrite the file don't throw exception
	 */
	
	public int copyFromLocal(String storageFilePath, String localFilePath) throws Exception {
		
		// System.out.println("copyFromLocal() storageFilePath : " + storageFilePath);
		log.debug("copyFromLocal() storageFilePath : {}", storageFilePath);
		// System.out.println("copyFromLocal() localFilePath : " + localFilePath);
		log.debug("copyFromLocal() localFilePath : {}", localFilePath);
		
		// works for async ByteArrayOutputStream baos = new ByteArrayOutputStream();
		
		//===============Read contents of local filet===================
		
		// works for async long startTime = System.currentTimeMillis();
		
		// works for async FileInputStream fis = new FileInputStream(localFilePath);
		// works for async byte[] buf = new byte[INTERNAL_BUFFER_SIZE];
		// works for async int len = 0;
		// works for async while ((len = fis.read(buf, 0, INTERNAL_BUFFER_SIZE)) > 0)
		// works for async 	baos.write(buf, 0, len);
		// works for async fis.close();
		// works for async byte[] bytea = baos.toByteArray();
		
		// works for async long endTime = System.currentTimeMillis();
		// System.out.println("copyFromLocal() Reading from local file. Time taken : " + (endTime - startTime));
		// log.debug("copyFromLocal() Reading from local file. Time taken : {}", (endTime - startTime));
		
		// baos.close();

		//===============Send POST request===================
		
		// <AppId>/<ArrayFolder>/<PageFolder/<PageFileName>
		// example "mydata_1_index_page-0.dat"
		
		String fileNameToPost = null;
		int idx = storageFilePath.indexOf(File.separator);
		fileNameToPost = storageFilePath.substring(idx + 1, storageFilePath.length());
		String relURL = "/" + fileNameToPost.replace(File.separator, "_");
		// System.out.println("copyFromLocal() fileNameToPost : " + fileNameToPost);
		// log.debug("copyFromLocal() fileNameToPost : {}", fileNameToPost);
		// System.out.println("copyFromLocal() relURL : " + relURL);
		// log.debug("copyFromLocal() relURL : {}", relURL);
		
		HttpPost postRequest = new HttpPost(relURL);
		// works for async String boundary = UUID.randomUUID().toString();
        MultipartEntityBuilder multipartEntityBuilder = MultipartEntityBuilder.create();
        // works for async multipartEntityBuilder.setBoundary(boundary); 
        // System.out.println("copyFromLocal() bytea.length : " + bytea.length);

        // Don't use - error -> "Arrays.copy() java heap out of space error" multipartEntityBuilder.addBinaryBody("file", bytea, ContentType.APPLICATION_OCTET_STREAM, fileNameToPost.replace(File.separator, "_"));

        File f = new File(localFilePath);
        multipartEntityBuilder.addBinaryBody("file", f, ContentType.APPLICATION_OCTET_STREAM, fileNameToPost.replace(File.separator, "_"));
        
        HttpEntity mEntity = multipartEntityBuilder.build();

        // works for async ByteArrayOutputStream baos2 = new ByteArrayOutputStream();
        // works for async mEntity.writeTo(baos2);

        // works for async HttpEntity nByteEntity = new NByteArrayEntity (baos2.toByteArray(), ContentType.MULTIPART_FORM_DATA);
        // works for async postRequest.setHeader("Content-Type", "multipart/form-data;boundary=" + boundary);
        // works for async postRequest.setEntity(nByteEntity);
       
		long startTimeRequest = System.currentTimeMillis();
        
	    // log.debug("copyFromLocal() Executing request to " + m_target + relURL);
		// Previous works Future<HttpResponse> futureHttpResponse = m_httpClient.execute(m_target, postRequest, null);
		
		//===============Get Result===================
		
		// Previous works HttpResponse httpResponse = futureHttpResponse.get();
		
		postRequest.setEntity(mEntity);
		postRequest.addHeader("Connection", "Keep-Alive");
		HttpResponse httpResponse = executeAndGet(postRequest, f.length());
		
		long endTimeRequest = System.currentTimeMillis();
		// System.out.println("copyFromLocal() Time taken Request & Response : " + (endTimeRequest - startTimeRequest));
		// log.debug("copyFromLocal() Time taken Request & Response : {}", (endTimeRequest - startTimeRequest));
		
		HttpEntity entity = httpResponse.getEntity();
		
		// Debugging
		// log.debug("copyFromLocal()----------------------------------------");
		// System.out.println("copyFromLocal()----------------------------------------");
		// log.debug("copyFromLocal() Status line : {}",httpResponse.getStatusLine());
		// System.out.println("copyFromLocal() Status line : " + httpResponse.getStatusLine());
		if (httpResponse.getStatusLine().getStatusCode() != 201) {
			// System.out.println("copyFromLocal() Response Status code is not 201. Response Status code : " + httpResponse.getStatusLine().getStatusCode());
			// log.debug("copyFromLocal() Response Status code is not 201. Response Status code : {}", httpResponse.getStatusLine().getStatusCode());
			// log.debug("copyFromLocal()----------------------------------------");
			// System.out.println("copyFromLocal()----------------------------------------");
			throw new IOException("Response Status code is not 201. Response Status code : " + httpResponse.getStatusLine().getStatusCode() + " relURL : " + relURL);
		}
		// Header[] headers = httpResponse.getAllHeaders();
		// log.debug("copyFromLocal() headers.length : {}", headers.length);
		// System.out.println("copyFromLocal() headers.length : " + headers.length);
		// for (int i = 0; i < headers.length; i++) {
			// log.debug("copyFromLocal() headers : {}", headers[i]);
			// System.out.println("copyFromLocal() headers : " + headers[i]);
		// }
		// log.debug("copyFromLocal()----------------------------------------");
		// System.out.println("copyFromLocal()----------------------------------------");
		
		return 0;
	}

	/* Method : copyIfExistsToLocal(String storageFilePath, String localFilePath)
	 * Assumptions for Storage system that does not support creation of directory given directory name
	 * Parameter : storageFilePath
	 * <AnyDummyStringWithoutSlash>/<ArrayFolder>/<PageFolder>/<PageFileName>
	 * <AnyDummyStringWithoutSlash> = <AppId> or "" (empty string) or any dummy string without File.separator ("/" or "\")
	 * 
	 * Construct file name from storageFilePath
	 * File name = <ArrayFolder>_<PageFolder>_<PageFileName> 
	 * example <ArrayFolder> = "mydata_1" <PageFolder> = "meta_data" <PageFileName> = "page-0.dat"
	 * file name = "mydata_1_meta_data_page-0.dat"
	 * 
	 * If the file doesn't exist in the Storage System, don't throw exception
	 */
	
	public int copyIfExistsToLocal(String storageFilePath, String localFilePath) throws Exception {

		// System.out.println("copyIfExistsToLocal() storageFilePath : " + storageFilePath);
		log.debug("copyIfExistsToLocal() storageFilePath : {}", storageFilePath);
		// System.out.println("copyIfExistsToLocal() localFilePath : " + localFilePath);
		log.debug("copyIfExistsToLocal() localFilePath : {}", localFilePath);
		
		//===============Send GET request===================
		
		// <AppId>/<ArrayFolder>/<PageFolder/<PageFileName>
		// example "mydata_1_index_page-0.dat"
		
		String fileNameToPost = null;
		int idx = storageFilePath.indexOf(File.separator);
		fileNameToPost = storageFilePath.substring(idx + 1, storageFilePath.length());
		String relURL = "/" + fileNameToPost.replace(File.separator, "_");
		// System.out.println("copyIfExistsToLocal() fileNameToPost : " + fileNameToPost);
		// log.debug("copyIfExistsToLocal() fileNameToPost : {}", fileNameToPost);
		// System.out.println("copyIfExistsToLocal() relURL : " + relURL);
		// log.debug("copyIfExistsToLocal() relURL : {}", relURL);
		
		HttpGet getRequest = new HttpGet(relURL);
		// log.debug("copyIfExistsToLocal() Executing request to " + m_target + relURL);
		
		long startTimeRequest = System.currentTimeMillis();
		
		// Previous works Future<HttpResponse> futureHttpResponse = m_httpClient.execute(m_target, getRequest, null);
		
		//===============Get Result===================

		// Previous works HttpResponse httpResponse = futureHttpResponse.get();
		
		getRequest.addHeader("Connection", "Keep-Alive");
		DMHttpResponse httpResponse = executeAndGet(getRequest);
		
		long endTimeRequest = System.currentTimeMillis();
		// System.out.println("copyIfExistsToLocal() Time taken Request & Response : " + (endTimeRequest - startTimeRequest));
		// log.debug("copyIfExistsToLocal() Time taken Request & Response : {}", (endTimeRequest - startTimeRequest));		
		
		HttpEntity entity = httpResponse.getHttpResponse().getEntity();
		
		// Debugging
		// log.debug("copyIfExistsToLocal()----------------------------------------");
		// System.out.println("copyIfExistsToLocal()----------------------------------------");
		// log.debug("copyIfExistsToLocal() Status line : {}",httpResponse.getStatusLine());
		// System.out.println("copyIfExistsToLocal() Status line : " + httpResponse.getStatusLine());
		if (httpResponse.getHttpResponse().getStatusLine().getStatusCode() != 200) {
			// System.out.println("copyIfExistsToLocal() Response Status code is not 200. Response Status code : " + httpResponse.getStatusLine().getStatusCode());
			// log.debug("copyIfExistsToLocal() Response Status code is not 200. Response Status code : {}", httpResponse.getStatusLine().getStatusCode());
			// log.debug("copyIfExistsToLocal()----------------------------------------");
			// System.out.println("copyIfExistsToLocal()----------------------------------------");
			return -1; // We don't throw exception here bacause if file doesn't exist then don't copy to local
		}

		// Header[] headers = httpResponse.getAllHeaders();
		// log.debug("copyIfExistsToLocal() headers.length : {}", headers.length);
		// System.out.println("copyIfExistsToLocal() headers.length : " + headers.length);
		// for (int i = 0; i < headers.length; i++) {
			// log.debug("copyIfExistsToLocal() headers : {}", headers[i]);
			// System.out.println("copyIfExistsToLocal() headers : " + headers[i]);
		// }
		// log.debug("copyIfExistsToLocal()----------------------------------------");
		// System.out.println("copyIfExistsToLocal()----------------------------------------");
		
		if (entity != null) {
			// System.out.println("copyIfExistsToLocal() Writing to local file");
			// log.debug("copyIfExistsToLocal() Writing to local file");
			
			// Previous works byte[] bytea = EntityUtils.toByteArray(entity);
			byte[] bytea = httpResponse.getByteArray();
			ByteArrayInputStream bais = new ByteArrayInputStream(bytea);
			
			long startTime = System.currentTimeMillis();
			
			FileOutputStream fos = new FileOutputStream(localFilePath);
			byte[] buf = new byte[INTERNAL_BUFFER_SIZE];
			int len = 0;
			while ((len = bais.read(buf, 0, INTERNAL_BUFFER_SIZE)) > 0)
				fos.write(buf, 0, len);
			fos.close();
			
			long endTime = System.currentTimeMillis();
			
			// System.out.println("copyIfExistsToLocal() Writing Time taken : " + (endTime - startTime));
			// log.debug("copyIfExistsToLocal() Writing Time taken : {}", (endTime - startTime));
			
			bais.close();
		} else {
			throw new IOException("Entity is null for request : " + getRequest + "Response Status Code is " + httpResponse.getHttpResponse().getStatusLine().getStatusCode() + " Status line : " + httpResponse.getHttpResponse().getStatusLine());
		}
		
		return 0;
	}

	/* 
	 * Method : isFileExists(String storageFilePath)
	 * 
	 * Assumptions for Storage system that does not support creation of directory given directory name
	 * Parameter : storageFilePath
	 * <AnyDummyStringWithoutSlash>/<ArrayFolder>/<PageFolder>/<PageFileName>
	 * <AnyDummyStringWithoutSlash> = <AppId> or "" (empty string) or any dummy string without File.separator ("/" or "\")
	 * 
	 * Construct file name from storageFilePath
	 * File name = <ArrayFolder>_<PageFolder>_<PageFileName> 
	 * example <ArrayFolder> = "mydata_1" <PageFolder> = "meta_data" <PageFileName> = "page-0.dat"
	 * file name = "mydata_1_meta_data_page-0.dat"
	 * 
	 * If the file doesn't exist in the Storage System, return false else return true
	 */
	public boolean isFileExists(String storageFilePath) throws Exception {

		// System.out.println("isFileExists() storageFilePath : " + storageFilePath);
		log.debug("isFileExists() storageFilePath : {}", storageFilePath);
		
		//===============Send GET request===================
		
		// <AppId>/<ArrayFolder>/<PageFolder/<PageFileName>
		// example "mydata_1_index_page-0.dat"
		
		String fileNameToPost = null;
		int idx = storageFilePath.indexOf(File.separator);
		fileNameToPost = storageFilePath.substring(idx + 1, storageFilePath.length());
		String relURL = "/" + fileNameToPost.replace(File.separator, "_");
		// System.out.println("isFileExists() fileNameToPost : " + fileNameToPost);
		// log.debug("isFileExists() fileNameToPost : {}", fileNameToPost);
		// System.out.println("isFileExists() relURL : " + relURL);
		// log.debug("isFileExists() relURL : {}", relURL);
		
		HttpGet getRequest = new HttpGet(relURL);
		// log.debug("isFileExists() Executing request to " + m_target + relURL);
		
		long startTimeRequest = System.currentTimeMillis();
		
		// Previous works Future<HttpResponse> futureHttpResponse = m_httpClient.execute(m_target, getRequest, null);
		
		//===============Get Result===================

		// Previous works HttpResponse httpResponse = futureHttpResponse.get();
		
		getRequest.addHeader("Connection", "Keep-Alive");
		HttpResponse httpResponse = executeAndGetForExistsCheckOnly(getRequest);
		
		long endTimeRequest = System.currentTimeMillis();
		// System.out.println("isFileExists() Time taken Request & Response : " + (endTimeRequest - startTimeRequest));
		// log.debug("isFileExists() Time taken Request & Response : {}", (endTimeRequest - startTimeRequest));		
		
		HttpEntity entity = httpResponse.getEntity();
		
		// Debugging
		// log.debug("isFileExists----------------------------------------");
		// System.out.println("isFileExists----------------------------------------");
		// log.debug("isFileExists() Status line : {}",httpResponse.getStatusLine());
		// System.out.println("isFileExists() Status line : " + httpResponse.getStatusLine());
		if (httpResponse.getStatusLine().getStatusCode() != 200) {
			// System.out.println("isFileExists() Response Status code is not 200. Response Status code : " + httpResponse.getStatusLine().getStatusCode());
			// log.debug("isFileExists() Response Status code is not 200. Response Status code : {}", httpResponse.getStatusLine().getStatusCode());
			// log.debug("isFileExists()----------------------------------------");
			// System.out.println("isFileExists()----------------------------------------");
			return false; // We don't throw exception here bacause if file doesn't exist then return false
		}

		// Header[] headers = httpResponse.getAllHeaders();
		// log.debug("isFileExists() headers.length : {}", headers.length);
		// System.out.println("isFileExists() headers.length : " + headers.length);
		// for (int i = 0; i < headers.length; i++) {
			// log.debug("isFileExists() headers : {}", headers[i]);
			// System.out.println("isFileExists() headers : " + headers[i]);
		// }
		// log.debug("isFileExists()----------------------------------------");
		// System.out.println("isFileExists()----------------------------------------");
	
		return true;		
	
	}
	
	/* Method : deleteIfExists(String storageFilePath)
	 * Assumptions for Storage system that does not support creation of directory given directory name
	 * Parameter : storageFilePath
	 * <AnyDummyStringWithoutSlash>/<ArrayFolder>/<PageFolder>/<PageFileName>
	 * <AnyDummyStringWithoutSlash> = <AppId> or "" (empty string) or any dummy string without File.separator ("/" or "\")
	 * 
	 * Construct file name from storageFilePath
	 * File name = <ArrayFolder>_<PageFolder>_<PageFileName> 
	 * example <ArrayFolder> = "mydata_1" <PageFolder> = "meta_data" <PageFileName> = "page-0.dat"
	 * file name = "mydata_1_meta_data_page-0.dat"
	 * 
	 * If the file doesn't exist in the Storage System, don't throw exception
	 */	
	
	public int deleteIfExists(String storageFilePath) throws Exception {

		// System.out.println("deleteIfExists() storageFilePath : " + storageFilePath);
		log.debug("deleteIfExists() storageFilePath : {}", storageFilePath);
		
		//===============Send GET request===================
		
		// <AppId>/<ArrayFolder>/<PageFolder>/<PageFileName>
		// example "mydata_1_index_page-0.dat"
		
		String fileNameToPost = null;
		int idx = storageFilePath.indexOf(File.separator);
		fileNameToPost = storageFilePath.substring(idx + 1, storageFilePath.length());
		String relURL = "/" + fileNameToPost.replace(File.separator, "_");
		// System.out.println("deleteIfExists() fileNameToPost : " + fileNameToPost);
		// log.debug("deleteIfExists() fileNameToPost : {}", fileNameToPost);		
		// System.out.println("deleteIfExists() relURL : " + relURL);
		// log.debug("deleteIfExists() relURL : {}", relURL);
		
		HttpDelete delRequest = new HttpDelete(relURL);
		// log.debug("deleteIfExists() Executing request to " + m_target + relURL);
		
		long startTimeRequest = System.currentTimeMillis();
		
		// Previous works Future<HttpResponse> futureHttpResponse = m_httpClient.execute(m_target, delRequest, null);
		
		//===============Get Result===================

		// Previous works HttpResponse httpResponse = futureHttpResponse.get();
		
		delRequest.addHeader("Connection", "Keep-Alive");
		HttpResponse httpResponse = executeAndGet(delRequest);
		
		long endTimeRequest = System.currentTimeMillis();
		// System.out.println("deleteIfExists() Time taken Request & Response : " + (endTimeRequest - startTimeRequest));
		// log.debug("deleteIfExists() Time taken Request & Response : {}", (endTimeRequest - startTimeRequest));
		
		HttpEntity entity = httpResponse.getEntity();
		
		// Debugging
		// log.debug("deleteIfExists()----------------------------------------");
		// System.out.println("deleteIfExists()----------------------------------------");
		// log.debug("deleteIfExists() Status line : {}", httpResponse.getStatusLine());
		// System.out.println("deleteIfExists() Status line : " + httpResponse.getStatusLine());
		if (httpResponse.getStatusLine().getStatusCode() != 202) {
			// System.out.println("deleteIfExists() Response Status code is not 202. Response Status code : " + httpResponse.getStatusLine().getStatusCode());
			// log.debug("deleteIfExists() Response Status code is not 202. Response Status code : {}", httpResponse.getStatusLine().getStatusCode());
			// log.debug("deleteIfExists()----------------------------------------");
			// System.out.println("deleteIfExists()----------------------------------------");
			return -1; // Don't throw exception --> throw new IOException("Response Status code is not 202. Response Status code : " + httpResponse.getStatusLine().getStatusCode() + " relURL : " + relURL);
		}

		// Header[] headers = httpResponse.getAllHeaders();
		// log.debug("deleteIfExists() headers.length : {}", headers.length);
		// System.out.println("deleteIfExists() headers.length : " + headers.length);
		// for (int i = 0; i < headers.length; i++) {
			// log.debug("deleteIfExists() headers : {}", headers[i]);
			// System.out.println("deleteIfExists() headers : " + headers[i]);
		// }
		// log.debug("deleteIfExists()----------------------------------------");
		// System.out.println("deleteIfExists()----------------------------------------");
		
		return 0;
	}
	
	/* Method : mkdirIfAbsent(String dir)
	 * Assumptions for Storage system that does not support creation of directory given directory name
	 * Parameter : dirPath
	 * <AnyDummyStringWithoutSlash>/<ArrayFolder>
	 * <AnyDummyStringWithoutSlash> = <AppId> or "" (empty string) or any dummy string without File.separator ("/" or "\")
	 * 
	 * return always true
	 */
	
	public boolean mkdirIfAbsent(String dir) throws Exception {
		return true;
	}

	/* Method : deleteDirIfExists(String dirPath)
	 * Assumptions for Storage system that does not support creation of directory given directory name
	 * Parameter : dirPath
	 * <AnyDummyStringWithoutSlash>/<ArrayFolder>
	 * <AnyDummyStringWithoutSlash> = <AppId> or "" (empty string) or any dummy string without File.separator ("/" or "\")
	 * 
	 * Construct file names
	 * File name = <ArrayFolder>_meta_data_page-<n>.dat (n = 1, 2, 3, ...) 
	 * example <ArrayFolder> = "mydata_1" file name = "mydata_1_meta_data_page-0.dat"
	 * 
	 * File name = <ArrayFolder>_index_page-<n>.dat (n = 1, 2, 3, ...) 
	 * example <ArrayFolder> = "mydata_1" file name = "mydata_1_index_page-0.dat"
	 * 
	 * File name = <ArrayFolder>_data_page-<n>.dat (n = 1, 2, 3, ...) 
	 * example <ArrayFolder> = "mydata_1" file name = "mydata_1_data_page-0.dat"
	 * 
	 * Delete all these files with file names with suffix from page-0.dat to till page-<n>.dat if the file exists (n = 1, 2, 3, ...)
	 * Delete all these files with file names <ArrayFolder>_meta_data_page-<n>.dat if the file exists (n = 1, 2, 3, ...)
	 * Delete all these files with file names <ArrayFolder>_index_page-<n>.dat if the file exists (n = 1, 2, 3, ...)
	 * Delete all these files with file names <ArrayFolder>_data_page-<n>.dat if the file exists (n = 1, 2, 3, ...)
	 * 
	 * If the files don't exist in the Storage System, don't throw exception
	 */
	
	public int deleteDirIfExists(String dirPath) throws Exception {

		// System.out.println("deleteDirIfExists() dirPath : " + dirPath);
		log.debug("deleteDirIfExists() dirPath : " + dirPath);
		
		// <AppId>/<ArrayFolder>
		// Construct file names
		// example "mydata_1_meta_data_page-0.dat"
		// example "mydata_1_index_page-0.dat"
		// example "mydata_1_data_page-0.dat"
		
		String arrayFolder = null;
		if (dirPath.endsWith(File.separator))
			dirPath = dirPath.substring(0, dirPath.length() - 1);
		int idx = dirPath.lastIndexOf(File.separator);
		arrayFolder = dirPath.substring(idx + 1, dirPath.length());
		String fileNameBase = arrayFolder.replace(File.separator, "_");
		// System.out.println("deleteDirIfExists() arrayFolder : " + arrayFolder);
		// log.debug("deleteDirIfExists() arrayFolder : {}", arrayFolder);
		// System.out.println("deleteDirIfExists() fileNameBase : " + fileNameBase);
		// log.debug("deleteDirIfExists() fileNameBase : {}", fileNameBase);
		
		boolean check = true;
		long i = 0;
		while (check) {
			String fileName = null;
			if (fileNameBase.endsWith("_"))
				fileName = fileNameBase + "meta_data_page-" + i + ".dat";
			else
				fileName = fileNameBase + "_meta_data_page-" + i + ".dat";

			int ret = isExists(fileName);
					
			if (ret != 0)
				break;
			else {
				delete(fileName);
				i++;
			}
		}
		
		i = 0;
		while (check) {
			String fileName = null;
			if (fileNameBase.endsWith("_"))
				fileName = fileNameBase + "index_page-" + i + ".dat";
			else
				fileName = fileNameBase + "_index_page-" + i + ".dat";
			
			int ret = isExists(fileName);
			if (ret != 0)
				break;
			else {
				delete(fileName);
				i++;
			}
		}
		
		i = 0;
		while (check) {
			String fileName = null;
			if (fileNameBase.endsWith("_"))
				fileName = fileNameBase + "data_page-" + i + ".dat";
			else
				fileName = fileNameBase + "_data_page-" + i + ".dat";
			
			int ret = isExists(fileName);
			if (ret != 0)
				break;
			else {
				delete(fileName);
				i++;
			}
		}
		
		return 0;
	}	
	
	public void close() throws Exception {
		
		// works for async closeHttpAsyncService();
		m_syncClient.close();
	}
	
	private int delete(String fileName) throws Exception {
		
		//===============Send GET request===================
		
		// <ArrayFolder>_<PageFolder_<PageFileName>
		// fileName : example "mydata_1_index_page-0.dat"
		
		// System.out.println("delete() fileName : " + fileName);
		// log.debug("delete() fileName : {}", fileName);
		String relURL = "/" + fileName;
		// System.out.println("delete() relURL : " + relURL);
		// log.debug("delete() relURL : {}", relURL);
		
		HttpDelete delRequest = new HttpDelete(relURL);
		// log.debug("delete() Executing request to " + m_target + relURL);
		
		long startTimeRequest = System.currentTimeMillis();
		
		// Previous works Future<HttpResponse> futureHttpResponse = m_httpClient.execute(m_target, delRequest, null);
		
		//===============Get Result===================

		// Previous works HttpResponse httpResponse = futureHttpResponse.get();
		
		delRequest.addHeader("Connection", "Keep-Alive");
		HttpResponse httpResponse = executeAndGet(delRequest);
		
		long endTimeRequest = System.currentTimeMillis();
		// System.out.println("delete() Time taken Request & Response : " + (endTimeRequest - startTimeRequest));
		// log.debug("delete() Time taken Request & Response : {}", (endTimeRequest - startTimeRequest));
		
		HttpEntity entity = httpResponse.getEntity();
		
		// Debugging
		// log.debug("delete()----------------------------------------");
		// System.out.println("delete()----------------------------------------");
		// log.debug("delete() Status line : {}",httpResponse.getStatusLine());
		// System.out.println("delete() Status line : " + httpResponse.getStatusLine());
		if (httpResponse.getStatusLine().getStatusCode() != 202) {
			// System.out.println("delete() Response Status code is not 202. Response Status code : " + httpResponse.getStatusLine().getStatusCode());
			// log.debug("delete() Response Status code is not 202. Response Status code : {}", httpResponse.getStatusLine().getStatusCode());
			// log.debug("delete()----------------------------------------");
			// System.out.println("delete()----------------------------------------");
			return -1; // Don't throw exception. File exists check is done by isExists() method
		}

		// Header[] headers = httpResponse.getAllHeaders();
		// log.debug("delete() headers.length : {}", headers.length);
		// System.out.println("delete() headers.length : " + headers.length);
		// for (int i = 0; i < headers.length; i++) {
			// log.debug("delete() headers : {}", headers[i]);
			// System.out.println("delete() headers : " + headers[i]);
		// }
		// log.debug("delete()----------------------------------------");
		// System.out.println("delete()----------------------------------------");
	
		return 0;
	}
	
	private int isExists(String fileName) throws Exception {

		//===============Send GET request===================
		
		// <ArrayFolder>_<PageFolder_<PageFileName>
		// fileName : example "mydata_1_index_page-0.dat"
		
		// System.out.println("isExists() fileName : " + fileName);
		// log.debug("isExists() fileName : {}", fileName);
		String relURL = "/" + fileName;
		// System.out.println("isExists() relURL : " + relURL);
		// log.debug("isExists() relURL : {}", relURL);
		
		HttpGet getRequest = new HttpGet(relURL);
		// log.debug("isExists() Executing request to " + m_target + relURL);
		
		long startTimeRequest = System.currentTimeMillis();
		
		// Previous works Future<HttpResponse> futureHttpResponse = m_httpClient.execute(m_target, getRequest, null);
		
		//===============Get Result===================

		// Previous works HttpResponse httpResponse = futureHttpResponse.get();
		
		getRequest.addHeader("Connection", "Keep-Alive");
		HttpResponse httpResponse = executeAndGetForExistsCheckOnly(getRequest);
		
		long endTimeRequest = System.currentTimeMillis();
		// System.out.println("isExists() Time taken Request & Response : " + (endTimeRequest - startTimeRequest));
		// log.debug("isExists() Time taken Request & Response : {}", (endTimeRequest - startTimeRequest));
		
		HttpEntity entity = httpResponse.getEntity();
		
		// Debugging
		// log.debug("isExists()----------------------------------------");
		// System.out.println("isExists()----------------------------------------");
		// log.debug("isExists() Status line : {}", httpResponse.getStatusLine());
		// System.out.println("isExists() Status line : " + httpResponse.getStatusLine());
		if (httpResponse.getStatusLine().getStatusCode() != 200) {
			// System.out.println("isExists() Response Status code is not 200. Response Status code : " + httpResponse.getStatusLine().getStatusCode());
			// log.debug("isExists() Response Status code is not 200. Response Status code : {}", httpResponse.getStatusLine().getStatusCode());
			// log.debug("isExists()----------------------------------------");
			// System.out.println("isExists()----------------------------------------");
			return -1; // Don't throw exception. Just return -1 or non-zero value to indicate file doesn't exist
		}

		// Header[] headers = httpResponse.getAllHeaders();
		// log.debug("isExists() headers.length : {}", headers.length);
		// System.out.println("isExists() headers.length : " + headers.length);
		// for (int i = 0; i < headers.length; i++) {
			// log.debug("isExists() headers : {}", headers[i]);
			// System.out.println("isExists() headers : " + headers[i]);
		// }
		// log.debug("isExists()----------------------------------------");
		// System.out.println("isExists()----------------------------------------");
		
		return 0;
	}	

	private DMHttpResponse executeAndGet(HttpGet request) throws Exception {
		
		gc();
		
		int count = 0;
		long rr = 1;
		String lastError = null;
		
		synchronized (this) {
			m_rr.inc();
			if (m_rr.value() > m_countHosts)
				m_rr.set(1);
			rr = m_rr.value();
		}
		
		int retry = 0;
		while (count < m_countHosts) {
			// System.out.println("executeAndGet(HttpGet request) rr : " + rr);
			// log.debug("executeAndGet(HttpGet request) rr : {}", rr);
			
			HttpHost target = m_targetMap.get("" + rr);
			
			try {
				m_semaphore.acquire();
				// works for async Future<HttpResponse> futureHttpResponse = m_httpClient.execute(target, request, null);
				// works for async HttpResponse httpResponse = futureHttpResponse.get();
				
				HttpResponse httpResponse = m_syncClient.execute(target, request);
				
				/* Previous works
				if (httpResponse.getStatusLine().getStatusCode() != 500)
					return httpResponse;
				else
					lastError = DMJson.error("SSH.EAG1.2", "Response Status Code is " + httpResponse.getStatusLine().getStatusCode() + " Status line : " + httpResponse.getStatusLine());
				*/
				
				if (200 == httpResponse.getStatusLine().getStatusCode()) {	
					HttpEntity getEntity = httpResponse.getEntity();
					if (getEntity != null) {
						byte[] byteaGet = EntityUtils.toByteArray(getEntity);
						// log.debug("executeAndGet(HttpGet request) Content-Length for request : " + request + " byteaGet : " + byteaGet.length);	
							
						return new DMHttpResponse(httpResponse, byteaGet);

					} else {
						lastError = DMJson.error("SSH.EAG1.2", "Unable to get file. Entity is null. Get Request is : " + request.getURI().toString() + " Response Status Code is " + httpResponse.getStatusLine().getStatusCode() + " Status line : " + httpResponse.getStatusLine());
					}
				} else if (404 == httpResponse.getStatusLine().getStatusCode())
					return new DMHttpResponse(httpResponse, null);
				else
					lastError = DMJson.error("SSH.EAG1.2", "Response Status Code is " + httpResponse.getStatusLine().getStatusCode() + " Status line : " + httpResponse.getStatusLine());
				
			} catch (Error | RuntimeException e) {
				e.printStackTrace();
				String error = DMUtil.serverErrMsg(e);
				lastError = DMJson.error("SSH.EAG1.1", error);
			} catch (Exception e) {
				e.printStackTrace();
				String error = DMUtil.serverErrMsg(e);
				lastError = DMJson.error("SSH.EAG1.1", error);
			} catch(Throwable e) {
				e.printStackTrace();
				String error = DMUtil.serverErrMsg(e);
				lastError = DMJson.error("SSH.EAG1.1", error);
			} finally {
				m_semaphore.release();
			}
			
			System.out.println("executeAndGet(HttpGet request) retry : " + retry);
			try {
				// cw double rand = Math.random();
				// cw Thread.sleep((long)(rand * 60000));
				Thread.sleep(1000);
			} catch (Exception e) {
				// do nothing
			}
			
			if (retry < m_maxRetry) {
				retry++;
			} else {
				retry = 0;
				
				count++;
	
				rr++;
				if (rr > m_countHosts)
					rr = 1;
			}
		}
		throw new DScabiException("Fatal exception from Storage System. All Hosts tried. Error from last try : " + lastError, "SSH.EAG1.1");
	}
	
	private HttpResponse executeAndGetForExistsCheckOnly(HttpGet request) throws Exception {
		
		gc();
		
		int count = 0;
		long rr = 1;
		String lastError = null;
		
		synchronized (this) {
			m_rr.inc();
			if (m_rr.value() > m_countHosts)
				m_rr.set(1);
			rr = m_rr.value();
		}
		
		int retry = 0;
		while (count < m_countHosts) {
			// System.out.println("executeAndGetForExistsCheckOnly(HttpGet request) rr : " + rr);
			// log.debug("executeAndGetForExistsCheckOnly(HttpGet request) rr : {}", rr);
			
			HttpHost target = m_targetMap.get("" + rr);
			
			try {
				m_semaphore.acquire();
				// works for async Future<HttpResponse> futureHttpResponse = m_httpClient.execute(target, request, null);
				// works for async HttpResponse httpResponse = futureHttpResponse.get();
				
				HttpResponse httpResponse = m_syncClient.execute(target, request);
						
				// Previous works if (httpResponse.getStatusLine().getStatusCode() != 500)
				if (200 == httpResponse.getStatusLine().getStatusCode() || 404 == httpResponse.getStatusLine().getStatusCode())	
					return httpResponse;
				else
					lastError = DMJson.error("SSH.EAG4.2", "Response Status Code is " + httpResponse.getStatusLine().getStatusCode() + " Status line : " + httpResponse.getStatusLine());

			} catch (Error | RuntimeException e) {
				e.printStackTrace();
				String error = DMUtil.serverErrMsg(e);
				lastError = DMJson.error("SSH.EAG4.1", error);
			} catch (Exception e) {
				e.printStackTrace();
				String error = DMUtil.serverErrMsg(e);
				lastError = DMJson.error("SSH.EAG4.1", error);
			} catch(Throwable e) {
				e.printStackTrace();
				String error = DMUtil.serverErrMsg(e);
				lastError = DMJson.error("SSH.EAG4.1", error);
			} finally {
				m_semaphore.release();
			}
			
			System.out.println("executeAndGetForExistsCheckOnly(HttpGet request) retry : " + retry);
			try {
				// cw double rand = Math.random();
				// cw Thread.sleep((long)(rand * 60000));
				Thread.sleep(1000);
			} catch (Exception e) {
				// do nothing
			}
			
			if (retry < m_maxRetry) {
				retry++;
			} else {
				retry = 0;
				
				count++;
	
				rr++;
				if (rr > m_countHosts)
					rr = 1;
			}
		}
		throw new DScabiException("Fatal exception from Storage System. All Hosts tried. Error from last try : " + lastError, "SSH.EAG4.1");
	}
	
	private HttpResponse executeAndGet(HttpPost request, long contentLength) throws Exception {
		
		gc();
		
		int count = 0;
		long rr = 1;
		String lastError = null;
		String getError = null;
		
		synchronized (this) {
			m_rr.inc();
			if (m_rr.value() > m_countHosts)
				m_rr.set(1);
			rr = m_rr.value();
		}
		
		boolean check = false;
		int retry = 0;
		HttpResponse httpResponse = null;
		while (count < m_countHosts) {
			// System.out.println("executeAndGet(HttpPost request) rr : " + rr);
			// log.debug("executeAndGet(HttpPost request) rr : {}", rr);
			
			HttpHost target = m_targetMap.get("" + rr);
			
			try {
				m_semaphore.acquire();
				// works for async Future<HttpResponse> futureHttpResponse = m_httpClient.execute(target, request, null);
				// works for async HttpResponse httpResponse = futureHttpResponse.get();
				
				httpResponse = m_syncClient.execute(target, request);
				
				// Previous works if (httpResponse.getStatusLine().getStatusCode() != 500)
				if (201 == httpResponse.getStatusLine().getStatusCode()) {
					check = true;
					// Previous works return httpResponse;
				}
				else
					lastError = DMJson.error("SSH.EAG2.2", "Response Status Code is " + httpResponse.getStatusLine().getStatusCode() + " Status line : " + httpResponse.getStatusLine());
				
			}  catch (Error | RuntimeException e) {
				e.printStackTrace();
				String error = DMUtil.serverErrMsg(e);
				lastError = DMJson.error("SSH.EAG2.1", error);
			} catch (Exception e) {
				e.printStackTrace();
				String error = DMUtil.serverErrMsg(e);
				lastError = DMJson.error("SSH.EAG2.1", error);
			} catch(Throwable e) {
				e.printStackTrace();
				String error = DMUtil.serverErrMsg(e);
				lastError = DMJson.error("SSH.EAG2.1", error);
			} finally {
				m_semaphore.release();
			}
			
			//========================Check if Content-Length matches================================
			
			// This code does HTTP GET of the above uploaded file to check if the file is correctly
			// uploaded in the Storage System and able to read the file from the Storage System without
			// any error
			
			// check is set to true only for 201 Created response. So send HTTP GET request for this case only

			if (check) {
			
				return httpResponse;
				
			/* OR
			// This works perfectly but do we need to really double check after file upload like this?
			
			HttpGet getRequest = null;
			getError = null;
			try {
				getRequest = new HttpGet(request.getURI());
				
				DMHttpResponse httpResponse3 = executeAndGet(getRequest);
						
				if (httpResponse3.getHttpResponse().getStatusLine().getStatusCode() != 200) 
					getError = DMJson.error("SSH.EAG2.2", "Unable to get file after file upload. Get Request is : " + getRequest.getURI().toString() + " Response Status Code is " + httpResponse3.getHttpResponse().getStatusLine().getStatusCode() + " Status line : " + httpResponse3.getHttpResponse().getStatusLine());
				else {
					HttpEntity getEntity = httpResponse3.getHttpResponse().getEntity();
					if (getEntity != null) {
						// Previous works byte[] byteaGet = EntityUtils.toByteArray(getEntity);
						byte[] byteaGet = httpResponse3.getByteArray();
						if (byteaGet.length == contentLength) {
							// log.debug("executeAndGet(HttpPost request) Content-Length matches for request : " + request + " byteaGet : " + byteaGet.length + " contentLength : " + contentLength);	
							return httpResponse;
						} else {
							log.error("executeAndGet(HttpPost request) Content-Length don't match for request : " + request + " byteaGet : " + byteaGet.length + " contentLength : " + contentLength);
							log.error("executeAndGet(HttpPost request) Content-Length don't match for request : " + request + " byteaGet : " + byteaGet.length + " contentLength : " + contentLength);							
							log.error("executeAndGet(HttpPost request) Content-Length don't match for request : " + request + " byteaGet : " + byteaGet.length + " contentLength : " + contentLength);
							log.error("executeAndGet(HttpPost request) Content-Length don't match for request : " + request + " byteaGet : " + byteaGet.length + " contentLength : " + contentLength);
							log.error("executeAndGet(HttpPost request) Content-Length don't match for request : " + request + " byteaGet : " + byteaGet.length + " contentLength : " + contentLength);
						
							getError = DMJson.error("SSH.EAG2.2", "Unable to get file after file upload. Content-Length don't match" + " byteaGet : " + byteaGet.length + " contentLength : " + contentLength + " Get Request is : " + getRequest.getURI().toString() + " Response Status Code is " + httpResponse3.getHttpResponse().getStatusLine().getStatusCode() + " Status line : " + httpResponse3.getHttpResponse().getStatusLine());
						}
					} else {
						getError = DMJson.error("SSH.EAG2.2", "Unable to get file after file upload. Entity is null. Get Request is : " + getRequest.getURI().toString() + " Response Status Code is " + httpResponse3.getHttpResponse().getStatusLine().getStatusCode() + " Status line : " + httpResponse3.getHttpResponse().getStatusLine());
					}
				}
 					
			}  catch (Error | RuntimeException e) {
				e.printStackTrace();
				String error = DMUtil.serverErrMsg(e);
				getError = DMJson.error("SSH.EAG2.1", error);
			} catch (Exception e) {
				e.printStackTrace();
				String error = DMUtil.serverErrMsg(e);
				getError = DMJson.error("SSH.EAG2.1", error);
			} catch(Throwable e) {
				e.printStackTrace();
				String error = DMUtil.serverErrMsg(e);
				getError = DMJson.error("SSH.EAG2.1", error);
			} finally {
				// do nothing
			}
			
			if (getError != null) {
				log.error("Unable to get file after file upload. Get Request is : " + getRequest.getURI().toString() + " Get Error : " + getError + " Error from last HTTP POST try of this file upload : " + lastError);							
			}
			*/
				
			} // End if (check)
			
			//========================End of code for check if Content-Length matches=================
			
			//=========================Delete file if exists of failed upload=========================
			
			// Send HTTP DELETE request in case of any error or exception, 500 response status code or any other response code
			// from the above HTTP POST request
			// In case of 201 response code, "return httpResponse" is in above code. If control still comes here
			// then Content-Length of HTTP POST and HTTP GET don't match. So proceed with sending HTTP DELETE request
			
			/* cw Do we need this HTTP DELETE below? Do we really need to delete? Can't we just proceed with re-upload of file?
			HttpDelete delRequest = null;
			String delError = null;
			try {
				
				delRequest = new HttpDelete(request.getURI());
				
				HttpResponse httpResponse2 = executeAndGet(delRequest);
				
			}  catch (Error | RuntimeException e) {
				e.printStackTrace();
				String error = DMUtil.serverErrMsg(e);
				delError = DMJson.error("SSH.EAG2.1", error);
			} catch (Exception e) {
				e.printStackTrace();
				String error = DMUtil.serverErrMsg(e);
				delError = DMJson.error("SSH.EAG2.1", error);
			} catch(Throwable e) {
				e.printStackTrace();
				String error = DMUtil.serverErrMsg(e);
				delError = DMJson.error("SSH.EAG2.1", error);
			} finally {
				// do nothing
			}
			
			if (delError != null)
				throw new DScabiException("Unable to delete file if exists of failed upload. Delete Request is : " + delRequest.getURI().toString() + " Delete Error : " + delError + " Error from last HTTP POST try of this file upload : " + lastError, "SSH.EAG2.1");			
			*/
			//==========================End of code for delete file if exists of failed upload=========
			
			System.out.println("executeAndGet(HttpPost request) retry : " + retry);
			try {
				// cw double rand = Math.random();
				// cw Thread.sleep((long)(rand * 60000));
				Thread.sleep(1000);
			} catch (Exception e) {
				// do nothing
			}
			
			if (retry < m_maxRetry) {
				retry++;
			} else {
				retry = 0;

				count++;
	
				rr++;
				if (rr > m_countHosts)
					rr = 1;
			}
		}
		throw new DScabiException("Fatal exception from Storage System. All Hosts tried. Error from last HTTP POST try of this file upload : " + lastError + " last Get Error : " + getError, "SSH.EAG2.1");
	}
	
	private HttpResponse executeAndGet(HttpDelete request) throws Exception {
		
		int count = 0;
		long rr = 1;
		String lastError = null;
		
		synchronized (this) {
			m_rr.inc();
			if (m_rr.value() > m_countHosts)
				m_rr.set(1);
			rr = m_rr.value();
		}
		
		int retry = 0;
		while (count < m_countHosts) {
			// System.out.println("executeAndGet(HttpDelete request) rr : " + rr);
			// log.debug("executeAndGet(HttpDelete request) rr : {}", rr);
			
			HttpHost target = m_targetMap.get("" + rr);
			
			try {
				m_semaphore.acquire();
				// works for async Future<HttpResponse> futureHttpResponse = m_httpClient.execute(target, request, null);
				// works for async HttpResponse httpResponse = futureHttpResponse.get();
				
				HttpResponse httpResponse = m_syncClient.execute(target, request);
				
				/* Previous works
				if (httpResponse.getStatusLine().getStatusCode() != 500)
					return httpResponse;
				else
					lastError = DMJson.error("SSH.EAG3.2", "Response Status Code is " + httpResponse.getStatusLine().getStatusCode() + " Status line : " + httpResponse.getStatusLine());
				*/
				
				// As of now, Seaweed filer doesn't return 404 for HTTP DELETE request
				/* cw for seaweedfs v0.70
				if (202 == httpResponse.getStatusLine().getStatusCode() || 404 == httpResponse.getStatusLine().getStatusCode())
					log.debug("executeAndGet(HttpDelete request) Response Status Code is " + httpResponse.getStatusLine().getStatusCode() + " Status line : " + httpResponse.getStatusLine());
					return httpResponse;
				else {
					log.debug("executeAndGet(HttpDelete request) Response Status Code is " + httpResponse.getStatusLine().getStatusCode() + " Status line : " + httpResponse.getStatusLine());
					lastError = DMJson.error("SSH.EAG3.2", "Response Status Code is " + httpResponse.getStatusLine().getStatusCode() + " Status line : " + httpResponse.getStatusLine());
				}
				*/
				
				// This is for seaweedfs v0.74
				if (500 == httpResponse.getStatusLine().getStatusCode() || 202 == httpResponse.getStatusLine().getStatusCode() || 404 == httpResponse.getStatusLine().getStatusCode()) {
					log.debug("executeAndGet(HttpDelete request) Response Status Code is " + httpResponse.getStatusLine().getStatusCode() + " Status line : " + httpResponse.getStatusLine());
					if (500 == httpResponse.getStatusLine().getStatusCode()) {
						HttpEntity getEntity = httpResponse.getEntity();
						if (getEntity != null) {
							byte[] byteaGet = EntityUtils.toByteArray(getEntity);
							String s = new String(byteaGet);
							log.debug("executeAndGet(HttpDelete request) Content for request : " + request + " s : " + s);	
							if (s.contains("no entry"))
								return httpResponse;
						}
					} else
						return httpResponse;
				}
				else {
					log.debug("executeAndGet(HttpDelete request) Response Status Code is " + httpResponse.getStatusLine().getStatusCode() + " Status line : " + httpResponse.getStatusLine());
					lastError = DMJson.error("SSH.EAG3.2", "Delete Request is : " + request.getURI().toString() + "Response Status Code is " + httpResponse.getStatusLine().getStatusCode() + " Status line : " + httpResponse.getStatusLine());
				
					HttpEntity getEntity = httpResponse.getEntity();
					if (getEntity != null) {
						byte[] byteaGet = EntityUtils.toByteArray(getEntity);
						String s = new String(byteaGet);
						log.debug("executeAndGet(HttpDelete request) Content for request : " + request + " s : " + s);	
					} else {
						lastError = DMJson.error("SSH.EAG3.3", "Entity is null. Delete Request is : " + request.getURI().toString() + " Response Status Code is " + httpResponse.getStatusLine().getStatusCode() + " Status line : " + httpResponse.getStatusLine());
					}
				}
			}  catch (Error | RuntimeException e) {
				e.printStackTrace();
				String error = DMUtil.serverErrMsg(e);
				lastError = DMJson.error("SSH.EAG3.1", error);
			} catch (Exception e) {
				e.printStackTrace();
				String error = DMUtil.serverErrMsg(e);
				lastError = DMJson.error("SSH.EAG3.1", error);
			} catch(Throwable e) {
				e.printStackTrace();
				String error = DMUtil.serverErrMsg(e);
				lastError = DMJson.error("SSH.EAG3.1", error);
			} finally {
				m_semaphore.release();
			}
			
			System.out.println("executeAndGet(HttpDelete request) retry : " + retry);
			try {
				// cw double rand = Math.random();
				// cw Thread.sleep((long)(rand * 60000));
				Thread.sleep(1000);
			} catch (Exception e) {
				// do nothing
			}
			
			if (retry < m_maxRetry) {
				retry++;
			} else {
				retry = 0;

				count++;
	
				rr++;
				if (rr > m_countHosts)
					rr = 1;
			}
		}
		throw new DScabiException("Fatal exception from Storage System. All Hosts tried. Error from last try : " + lastError, "SSH.EAG3.1");
	}

	private static int startHttpAsyncService() {
		
		m_referenceCount.inc();
		
		synchronized(m_httpClient) {
			if (false == m_isStarted) {
				m_httpClient.start();
				m_isStarted = true;
			}
		}
		return 0;
	}

	private static int closeHttpAsyncService() throws IOException {
		
		synchronized (m_referenceCount) {
			m_referenceCount.dec();
			if (m_referenceCount.value() <= 0) {
				synchronized(m_httpClient) {
					if (m_isStarted) {
						m_httpClient.close();
						m_isStarted = false;
					}
				}
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

	public int decClientPortCount() {
		
		if (m_clientConnectionsPerRoute > 0) {
			m_clientConnectionsPerRoute--;
			// m_clientPortLimiter.dec();
		}

		return 0;
	}
	
	public int incClientPortCount() {
		
		if (m_clientConnectionsPerRoute < m_maxClientConnectionsPerRoute) {
			m_clientConnectionsPerRoute++;
			// m_clientPortLimiter.inc();
		}

		return 0;
	}	
	
}

	



