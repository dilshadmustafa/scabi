/**
 * @author Dilshad Mustafa
 * Copyright (c) Dilshad Mustafa
 * All Rights Reserved.
 * @since 07-Feb-2016
 * File Name : DFile.java
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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.ParseException;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dilmus.dilshad.scabi.common.DMJsonHelper;
import com.dilmus.dilshad.scabi.common.DMUtil;
import com.dilmus.dilshad.scabi.common.DScabiException;
import com.dilmus.dilshad.scabi.db.DBackFile;
import com.dilmus.dilshad.scabi.db.DDB;
import com.dilmus.dilshad.scabi.deprecated.DBackFileOld;
import com.dilmus.dilshad.scabi.deprecated.DDBOld;

/**
 * @author Dilshad Mustafa
 *
 */
public class DFile {

	final Logger log = LoggerFactory.getLogger(DFile.class); 
	private DMeta m_meta = null;
	private CloseableHttpClient m_httpClient = null;
	private HttpHost m_metaTarget = null;
	private String m_metaHost = null;
	private String m_metaPort = null;
	private DNamespace m_namespace = null;
	private String m_strNamespace = null;
	//private boolean m_firstTime = true;

	private DDB m_ddb = null;
    private DBackFile m_dbfile = null;
	
	public DFile(DMeta meta) throws IOException {
		m_metaHost = meta.getHost();
		m_metaPort = meta.getPort();
		
		try {
			m_httpClient = HttpClientBuilder.create().build();
			m_metaTarget = new HttpHost(m_metaHost, Integer.parseInt(m_metaPort), "http");
			m_meta = meta;
			//m_firstTime = true;

			m_namespace = null;
			m_strNamespace = null;
    		
    	} catch (Exception e) {
			e.printStackTrace();
    		if (null == m_httpClient) 
    			m_httpClient.close();
    		throw e;
		}
		
	}

	public int close() throws IOException {
		m_dbfile.close();
		m_ddb.close();
		m_httpClient.close();

		m_meta = null;
		m_httpClient = null;
		m_metaTarget = null;
		m_metaHost = null;
		m_metaPort = null;
		m_namespace = null;
		m_strNamespace = null;
		//m_firstTime = true;

		m_ddb = null;
	    m_dbfile = null;
		
		return 0;
	}

	public int setNamespace(String strNamespace) throws ParseException, IOException, DScabiClientException, DScabiException {
		m_namespace = null;
		m_strNamespace = null;
		//m_firstTime = true;

		if (null != m_dbfile)
			m_dbfile.close();
		if (null != m_ddb)
			m_ddb.close();

		if (null == strNamespace) {
			throw new DScabiClientException("strNamespace is null", "DFE.PUT.1");
		}
		
		DNamespace namespace = m_meta.getNamespace(strNamespace, DNamespace.FILE);
		/*
		if (null == namespace) {
			throw new DScabiClientException("namespace is null", "DFE.SNE.1");
		}
		if (false == namespace.getType().equals("File")) {
			throw new DScabiClientException("Namespace type is not File type. Actual type : " + namespace.getType(), "DFE.SNE.2");
		}
		*/
	
		m_ddb = new DDB(namespace.getHost(), namespace.getPort(), namespace.getSystemSpecificName());
		m_dbfile = new DBackFile(m_ddb);

		m_namespace = namespace;
		m_strNamespace = strNamespace;

		return 0;
	}
	
	public int setNamespace(DNamespace namespace) throws DScabiClientException {
		m_namespace = null;
		m_strNamespace = null;
		//m_firstTime = true;

		if (null != m_dbfile)
			m_dbfile.close();
		if (null != m_ddb)
			m_ddb.close();


		if (null == namespace) {
			throw new DScabiClientException("namespace is null", "DFE.SNE2.1");
		}
		if (false == namespace.getType().equals(DNamespace.FILE)) {
			throw new DScabiClientException("Namespace type is not File type. Actual type : " + namespace.getType(), "DFE.SNE2.2");
		}
		
		m_ddb = new DDB(namespace.getHost(), namespace.getPort(), namespace.getSystemSpecificName());
		m_dbfile = new DBackFile(m_ddb);

		m_namespace = namespace;
		m_strNamespace = namespace.getName();

		return 0;
	}
	
	/* Previously working
	public DNamespace getNamespace(String strNamespace) throws DScabiException, ParseException, IOException, DScabiClientException {
		String metaType = "File";
		HttpPost postRequest = new HttpPost("/Meta/Namespace/Get");
		String myString = "{ \"Type\" : \"" +  metaType + "\", \"Namespace\" : \"" + strNamespace + "\" }";
	    StringEntity params =new StringEntity(myString);
	    
	    postRequest.addHeader("content-type", "application/json");
	    postRequest.setEntity(params);
	            			
		log.debug("getNamespace() executing request to " + m_metaTarget + "/Meta/Namespace/Get");

		// works HttpResponse httpResponse = httpClient.execute(target, getRequest);
		HttpResponse httpResponse = m_httpClient.execute(m_metaTarget, postRequest);
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
			throw new DScabiClientException("Response is null for Type : File, Namespace : " + strNamespace, "DFE.GNE.1");
		}
		
		if (DMJsonHelper.isError(jsonString)) {
			throw new DScabiClientException("Unable to get namespace " + strNamespace + " Error message : " + jsonString, "DFE.GNE.2");
		}
		
		DNamespace namespace = new DNamespace(jsonString);
		
		DNamespace namespace = m_meta.getNamespace(strNamespace);
		if (false == namespace.getType().equals("File"))
			throw new DScabiException("Namespace is not of File type", "DFE.GNE.1");
		return namespace;
		
	}
	*/
	
	/* Previously working
	public DNamespace findOneNamespace() throws ParseException, IOException, DScabiClientException, DScabiException {
		
		String metaType = "File";
		HttpPost postRequest = new HttpPost("/Meta/Namespace/FindOne");
		String myString = "{ \"Type\" : \"" +  metaType + "\" }";
	    StringEntity params =new StringEntity(myString);
	    
	    postRequest.addHeader("content-type", "application/json");
	    postRequest.setEntity(params);
	            			
		log.debug("findOneNamespace() executing request to " + m_metaTarget + "/Meta/Namespace/FindOne");

		// works HttpResponse httpResponse = httpClient.execute(target, getRequest);
		HttpResponse httpResponse = m_httpClient.execute(m_metaTarget, postRequest);
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
			throw new DScabiClientException("Response is null for Type : File", "DFE.FON.1");
		}
		
		if (DMJsonHelper.isError(jsonString)) {
			throw new DScabiClientException("Unable to get namespace. Error message : " + jsonString, "DFE.FON.2");
		}

		DNamespace namespace = new DNamespace(jsonString);
		
		DNamespace namespace = m_meta.findOneFileNS();
		return namespace;
		
	}
	*/
	
	public DNamespace setOneNamespace() throws ParseException, IOException, DScabiClientException, DScabiException {
		DNamespace namespace = m_meta.findOneFileNS();
		setNamespace(namespace);
		return namespace;
	}
	
	/* converted to private method : dbfile.removeFilesIncompleteMetaData(fileName, strFileID)
	public int removeFilesIncompleteMetaData(String fileName, String strFileID) throws ScabiClientException {
		if (m_firstTime) {
			if (null == m_namespace) {
				throw new ScabiClientException("Namespace is not set", "DFE.RFI.1");
			}
			ddb = new DBackDB(m_namespace.getHost(), m_namespace.getPort(), m_namespace.getSystemSpecificName());
			m_dbfile = new DBackFile(m_ddb);
	        
			m_firstTime = false;
		}
		return 	m_dbfile.removeFilesIncompleteMetaData(fileName, strFileID);

	}
	*/
	
	private int removeAllFilesIncompleteMetaData() throws DScabiClientException {
		/*
		if (m_firstTime) {
			if (null == m_namespace) {
				throw new DScabiClientException("Namespace is not set", "DFE.PUT.1");
			}
			m_ddb = new DDB(m_namespace.getHost(), m_namespace.getPort(), m_namespace.getSystemSpecificName());
			m_dbfile = new DBackFile(m_ddb);
	        
			m_firstTime = false;
		}
		*/
		if (null == m_namespace) {
			throw new DScabiClientException("Namespace is not set", "DFE.PUT.1");
		}
		return 	m_dbfile.removeAllFilesIncompleteMetaData();

	}
	
	private boolean isValidMetaData(String fileName, String strFileID) throws IOException, DScabiClientException, DScabiException {
		/*
		if (m_firstTime) {
			if (null == m_namespace) {
				throw new DScabiClientException("Namespace is not set", "DFE.IVM.1");
			}
			m_ddb = new DDB(m_namespace.getHost(), m_namespace.getPort(), m_namespace.getSystemSpecificName());
			m_dbfile = new DBackFile(m_ddb);
	        
			m_firstTime = false;
		}
		*/
		if (null == m_namespace) {
			throw new DScabiClientException("Namespace is not set", "DFE.IVM.1");
		}
		return 	m_dbfile.isValidMetaData(fileName, strFileID);

	}
	
	/* Previously used working
	public int put(String fileName, String fullFilePath) throws IOException, DScabiClientException, DScabiException, java.text.ParseException {
		long time1;
		long time2;

		if (m_firstTime) {
			if (null == m_namespace) {
				throw new DScabiClientException("Namespace is not set", "DFE.PUT.1");
			}
			m_ddb = new DDB(m_namespace.getHost(), m_namespace.getPort(), m_namespace.getSystemSpecificName());
			m_dbfile = new DBackFile(m_ddb);
	        
			m_firstTime = false;
		}
		
        time1 = System.currentTimeMillis();
        m_dbfile.put(fileName, fullFilePath, "File", "File");
        
        time2 = System.currentTimeMillis();

        log.debug("put() Upload time taken : time2 - time1 : " + (time2 - time1));
		
		return 0;
	}
	*/
	
	/* Previously used working
	public int put(String fileName, InputStream fromStream) throws IOException, DScabiClientException, DScabiException, java.text.ParseException {
		long time1;
		long time2;

		if (m_firstTime) {
			if (null == m_namespace) {
				throw new DScabiClientException("Namespace is not set", "DFE.PUT2.1");
			}
			m_ddb = new DDB(m_namespace.getHost(), m_namespace.getPort(), m_namespace.getSystemSpecificName());
			m_dbfile = new DBackFile(m_ddb);
	        
			m_firstTime = false;
		}
		
        time1 = System.currentTimeMillis();
        m_dbfile.put(fileName, fromStream, "File", "File");
        
        time2 = System.currentTimeMillis();

        log.debug("put() Upload time taken : time2 - time1 : " + (time2 - time1));
		
		return 0;
	}
	*/
	
	/* Not used
	public int put(String fileName, String fullFilePath) throws IOException, DScabiClientException, DScabiException, java.text.ParseException {
		long time1;
		long time2;
		
        time1 = System.currentTimeMillis();
        if (DMUtil.isNamespaceURLStr(fullFilePath)) {
        	putByNamespaceURLStr(fileName, fullFilePath);
        } else {
        	this.putByFilePath(fileName, fullFilePath);
        }
        time2 = System.currentTimeMillis();

        log.debug("put() Upload time taken : time2 - time1 : " + (time2 - time1));
		
		return 0;
	}
	*/

	public int put(String fileName, InputStream fromStream) throws IOException, DScabiClientException, DScabiException, java.text.ParseException {
		long time1;
		long time2;

        time1 = System.currentTimeMillis();
        
		if (DMUtil.isNamespaceURLStr(fileName)) {
			String strNamespace1 = DMUtil.getNamespaceStr(fileName);
			String strResourceName1 = DMUtil.getResourceName(fileName);
			
			DNamespace namespace1 = m_meta.getNamespace(strNamespace1, DNamespace.FILE);
			/*
			if (false == namespace1.getType().equals("File")) {
				throw new DScabiClientException("Namespace type is not File type. Actual type : " + namespace1.getType(), "DFE.SNE2.2");

			}
			*/
			DDB ddb1 = new DDB(namespace1.getHost(), namespace1.getPort(), namespace1.getSystemSpecificName());
			DBackFile dbfile1 = new DBackFile(ddb1);

			dbfile1.put(strResourceName1, fromStream, "File", "File");
			dbfile1.close();
			ddb1.close();
			
		}
		else {
			/*
			if (m_firstTime) {
				if (null == m_namespace) {
					throw new DScabiClientException("Namespace is not set", "DFE.PUT2.1");
				}
				m_ddb = new DDB(m_namespace.getHost(), m_namespace.getPort(), m_namespace.getSystemSpecificName());
				m_dbfile = new DBackFile(m_ddb);
		        
				m_firstTime = false;
			}
			*/
			if (null == m_namespace) {
				throw new DScabiClientException("Namespace is not set", "DFE.PUT2.1");
			}
			m_dbfile.put(fileName, fromStream, "File", "File");
		}
        time2 = System.currentTimeMillis();

        log.debug("put() Upload time taken : time2 - time1 : " + (time2 - time1));
		
		return 0;
	}

	public int copy(String fileName1, String fileName2) throws IOException, DScabiClientException, DScabiException, java.text.ParseException {
		long time1;
		long time2;
		
		if (false == DMUtil.isNamespaceURLStr(fileName2))
			throw new DScabiClientException("fileName2 is not proper namespace URL string", "DFE.PBN.1");
		String strNamespace2 = DMUtil.getNamespaceStr(fileName2);
		String strResourceName2 = DMUtil.getResourceName(fileName2);
		
		DNamespace namespace2 = m_meta.getNamespace(strNamespace2, DNamespace.FILE);
		/*
		if (false == namespace2.getType().equals("File")) {
			throw new DScabiClientException("Namespace type is not File type. Actual type : " + namespace2.getType(), "DFE.SNE2.2");
		}
		*/
        time1 = System.currentTimeMillis();
        
		if (DMUtil.isNamespaceURLStr(fileName1)) {
			String strNamespace1 = DMUtil.getNamespaceStr(fileName1);
			String strResourceName1 = DMUtil.getResourceName(fileName1);
			
			DNamespace namespace1 = m_meta.getNamespace(strNamespace1, DNamespace.FILE);
			/*
			if (false == namespace1.getType().equals("File")) {
				throw new DScabiClientException("Namespace type is not File type. Actual type : " + namespace1.getType(), "DFE.SNE2.2");

			}
			*/
			DDB ddb2 = new DDB(namespace2.getHost(), namespace2.getPort(), namespace2.getSystemSpecificName());
			DBackFile dbfile2 = new DBackFile(ddb2);

			DDB ddb1 = new DDB(namespace1.getHost(), namespace1.getPort(), namespace1.getSystemSpecificName());
			DBackFile dbfile1 = new DBackFile(ddb1);

			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			dbfile2.get(strResourceName2, baos);
			ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
			baos.close();
			dbfile1.put(strResourceName1, bais, "File", "File");
			
			bais.close();
			dbfile1.close();
			ddb1.close();
			dbfile2.close();
			ddb2.close();
			
		}
		else {
			/*
			if (m_firstTime) {
				if (null == m_namespace) {
					throw new DScabiClientException("Namespace is not set", "DFE.PUT2.1");
				}
				m_ddb = new DDB(m_namespace.getHost(), m_namespace.getPort(), m_namespace.getSystemSpecificName());
				m_dbfile = new DBackFile(m_ddb);
		        
				m_firstTime = false;
			}
			*/
			if (null == m_namespace) {
				throw new DScabiClientException("Namespace is not set", "DFE.PUT2.1");
			}
			DDB ddb2 = new DDB(namespace2.getHost(), namespace2.getPort(), namespace2.getSystemSpecificName());
			DBackFile dbfile2 = new DBackFile(ddb2);

			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			dbfile2.get(strResourceName2, baos);
			ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
			baos.close();
			
			m_dbfile.put(fileName1, bais, "File", "File");
			
			bais.close();
			dbfile2.close();
			ddb2.close();
        
		}
        time2 = System.currentTimeMillis();

        log.debug("put() Upload time taken : time2 - time1 : " + (time2 - time1));
		
		return 0;
	}

	public int put(String fileName, String fullFilePath) throws IOException, DScabiClientException, DScabiException, java.text.ParseException {
		long time1;
		long time2;

        time1 = System.currentTimeMillis();

		if (DMUtil.isNamespaceURLStr(fileName)) {
			String strNamespace1 = DMUtil.getNamespaceStr(fileName);
			String strResourceName1 = DMUtil.getResourceName(fileName);
			
			DNamespace namespace1 = m_meta.getNamespace(strNamespace1, DNamespace.FILE);
			/*
			if (false == namespace1.getType().equals("File")) {
				throw new DScabiClientException("Namespace type is not File type. Actual type : " + namespace1.getType(), "DFE.SNE2.2");

			}
			*/
			DDB ddb1 = new DDB(namespace1.getHost(), namespace1.getPort(), namespace1.getSystemSpecificName());
			DBackFile dbfile1 = new DBackFile(ddb1);

			dbfile1.put(strResourceName1, fullFilePath, "File", "File");
			dbfile1.close();
			ddb1.close();
			
		}
		else {
			/*
			if (m_firstTime) {
				if (null == m_namespace) {
					throw new DScabiClientException("Namespace is not set", "DFE.PUT.1");
				}
				m_ddb = new DDB(m_namespace.getHost(), m_namespace.getPort(), m_namespace.getSystemSpecificName());
				m_dbfile = new DBackFile(m_ddb);
		        
				m_firstTime = false;
			}
			*/
			if (null == m_namespace) {
				throw new DScabiClientException("Namespace is not set", "DFE.PUT.1");
			}
	        m_dbfile.put(fileName, fullFilePath, "File", "File");
		}
        time2 = System.currentTimeMillis();

        log.debug("put() Upload time taken : time2 - time1 : " + (time2 - time1));
		
		return 0;
	}

	/* Previously used working
	public int get(String fileName, String fullFilePath) throws IOException, DScabiClientException, DScabiException {
		long time1;
		long time2;

		if (m_firstTime) {
			if (null == m_namespace) {
				throw new DScabiClientException("Namespace is not set", "DFE.GET.1");
			}
			m_ddb = new DDB(m_namespace.getHost(), m_namespace.getPort(), m_namespace.getSystemSpecificName());
			m_dbfile = new DBackFile(m_ddb);
	        
			m_firstTime = false;
		}
		
        time1 = System.currentTimeMillis();
        m_dbfile.get(fileName, fullFilePath);
        
        time2 = System.currentTimeMillis();
        log.debug("get() Download time taken : time2 - time1 : " + (time2 - time1));
		
		return 0;
	}
	*/
	
	/* Previously used working
	public int get(String fileName, OutputStream toStream) throws IOException, DScabiClientException, DScabiException {
		long time1;
		long time2;

		if (m_firstTime) {
			if (null == m_namespace) {
				throw new DScabiClientException("Namespace is not set", "DFE.GET2.1");
			}
			m_ddb = new DDB(m_namespace.getHost(), m_namespace.getPort(), m_namespace.getSystemSpecificName());
			m_dbfile = new DBackFile(m_ddb);
	        
			m_firstTime = false;
		}
		
        time1 = System.currentTimeMillis();
        m_dbfile.get(fileName, toStream);
        
        time2 = System.currentTimeMillis();
        log.debug("get() Download time taken : time2 - time1 : " + (time2 - time1));
		
		return 0;
	}
	*/
	
	public int get(String fileName, String fullFilePath) throws IOException, DScabiClientException, DScabiException, java.text.ParseException {
		long time1;
		long time2;

        time1 = System.currentTimeMillis();

		if (DMUtil.isNamespaceURLStr(fileName)) {
			String strNamespace1 = DMUtil.getNamespaceStr(fileName);
			String strResourceName1 = DMUtil.getResourceName(fileName);
			
			DNamespace namespace1 = m_meta.getNamespace(strNamespace1, DNamespace.FILE);
			/*
			if (false == namespace1.getType().equals("File")) {
				throw new DScabiClientException("Namespace type is not File type. Actual type : " + namespace1.getType(), "DFE.SNE2.2");

			}
			*/
			DDB ddb1 = new DDB(namespace1.getHost(), namespace1.getPort(), namespace1.getSystemSpecificName());
			DBackFile dbfile1 = new DBackFile(ddb1);

			dbfile1.get(strResourceName1, fullFilePath);
			dbfile1.close();
			ddb1.close();
			
		}
		else {
			/*
			if (m_firstTime) {
				if (null == m_namespace) {
					throw new DScabiClientException("Namespace is not set", "DFE.GET.1");
				}
				m_ddb = new DDB(m_namespace.getHost(), m_namespace.getPort(), m_namespace.getSystemSpecificName());
				m_dbfile = new DBackFile(m_ddb);
		        
				m_firstTime = false;
			}
			*/
			if (null == m_namespace) {
				throw new DScabiClientException("Namespace is not set", "DFE.GET.1");
			}
	        m_dbfile.get(fileName, fullFilePath);
		}
        time2 = System.currentTimeMillis();
        log.debug("get() Download time taken : time2 - time1 : " + (time2 - time1));
		
		return 0;
	}

	public int get(String fileName, OutputStream toStream) throws IOException, DScabiClientException, DScabiException {
		long time1;
		long time2;

        time1 = System.currentTimeMillis();

		if (DMUtil.isNamespaceURLStr(fileName)) {
			String strNamespace1 = DMUtil.getNamespaceStr(fileName);
			String strResourceName1 = DMUtil.getResourceName(fileName);
			
			DNamespace namespace1 = m_meta.getNamespace(strNamespace1, DNamespace.FILE);
			/*
			if (false == namespace1.getType().equals("File")) {
				throw new DScabiClientException("Namespace type is not File type. Actual type : " + namespace1.getType(), "DFE.SNE2.2");

			}
			*/
			DDB ddb1 = new DDB(namespace1.getHost(), namespace1.getPort(), namespace1.getSystemSpecificName());
			DBackFile dbfile1 = new DBackFile(ddb1);

			dbfile1.get(strResourceName1, toStream);
			dbfile1.close();
			ddb1.close();
			
		}
		else {
			/*
			if (m_firstTime) {
				if (null == m_namespace) {
					throw new DScabiClientException("Namespace is not set", "DFE.GET2.1");
				}
				m_ddb = new DDB(m_namespace.getHost(), m_namespace.getPort(), m_namespace.getSystemSpecificName());
				m_dbfile = new DBackFile(m_ddb);
		        
				m_firstTime = false;
			}
			*/
			if (null == m_namespace) {
				throw new DScabiClientException("Namespace is not set", "DFE.GET2.1");
			}
	        m_dbfile.get(fileName, toStream);
		}
        time2 = System.currentTimeMillis();
        log.debug("get() Download time taken : time2 - time1 : " + (time2 - time1));
		
		return 0;
	}

}
