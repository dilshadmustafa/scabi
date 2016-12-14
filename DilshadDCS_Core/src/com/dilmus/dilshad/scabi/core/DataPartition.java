/**
 * @author Dilshad Mustafa
 * Copyright (c) Dilshad Mustafa
 * All Rights Reserved.
 * Created 15-Sep-2016
 * File Name : DataPartition.java
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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dilmus.dilshad.scabi.common.DMCounter;
import com.dilmus.dilshad.scabi.common.DMStdStorageHandler;
import com.dilmus.dilshad.scabi.common.DScabiException;
import com.dilmus.dilshad.storage.IStorageHandler;
import com.dilmus.dilshad.scabi.common.DMUtil;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.leansoft.bigqueue.BigArrayImpl;
import com.leansoft.bigqueue.IBigArray;

/**
 * @author Dilshad Mustafa
 *
 */
public class DataPartition implements Iterable<DataElement> {

	private final Logger log = LoggerFactory.getLogger(DataPartition.class);
	private static final Logger logs = LoggerFactory.getLogger(DataPartition.class); // log for static methods
	
	private IBigArray m_bigArray = null;
	private IShuffle m_shuffle = null;
	private ObjectMapper m_objectMapper = new ObjectMapper();
	private long m_lastIndex = -1;
	private long m_currentIndex = -1;
	private DataContext m_context = null;
	private Dson m_dson = null;
	private Dson m_fieldsDson = null;
	private DataElement m_dataElement = null;
	private boolean m_enableZip = true;
	
	// for zip purposes
    ByteArrayOutputStream m_forzip_zipbyteostream = null;
    GZIPOutputStream m_forzip_gzipostream = null;
    // for unzip purposes
    ByteArrayOutputStream m_forunzip_unzipbyteostream = null;
    byte[] m_forunzip_unzipbuffer = null;

	private DMCounter m_gcCounter = new DMCounter();
	
	private final static String M_DEFAULT_ENCODING = "UTF-8";
	
	private String m_dataId = null;
	private String m_partitionId = null;
	private int m_pageSize = 0;
	private String m_storageDirPath = null;
	private String m_arrayFolder = null;
	private String m_localDirPath = null;

	IStorageHandler m_storageHandler = null;
	
	private int m_operationType = -1; // -1 - not set, 0 - append operation, 1 - get operation
	private boolean m_isDirectorySupported = true; // does Storage system support directory creation given directory name
	
	public void setDirectorySupported(boolean isDirectorySupported) {
		m_isDirectorySupported = isDirectorySupported;
	}
	
	private int createPartitionIdFile(String storageDirPath, String arrayFolder, String localDirPath, IStorageHandler storageHandler) throws IOException {
		
		String localFilePath = null;
		if (localDirPath.endsWith(File.separator)) {
			localFilePath = localDirPath + arrayFolder + ".txt";
		} else {
			localFilePath = localDirPath + File.separator + arrayFolder + ".txt";
		}
		
		FileOutputStream fos = new FileOutputStream(localFilePath);
		OutputStreamWriter osw = new OutputStreamWriter(fos);
		BufferedWriter bw = new BufferedWriter(osw);

		Date d = new Date();
		bw.write(d.toString());
		
		bw.close();
		osw.close();
		fos.close();
		
		String partitionFilePath = null;
		if (storageDirPath.endsWith(File.separator)) {
			partitionFilePath = storageDirPath + arrayFolder + ".txt";
		} else {
			partitionFilePath = storageDirPath + File.separator + arrayFolder + ".txt";
		}
		
		try {
			storageHandler.copyFromLocal(partitionFilePath, localFilePath);
		} catch (Exception e) {
			throw new IOException(e);
		}
		
		Path path = Paths.get(localFilePath);
		Files.deleteIfExists(path);
		
		log.debug("createPartitionIdFile() Partition Id file created : {}", partitionFilePath);
		
		return 0;
	}
	
	private static int deletePartitionIdFile(String storageDirPath, String arrayFolder, IStorageHandler storageHandler) throws IOException {
		
		String partitionFilePath = null;
		if (storageDirPath.endsWith(File.separator)) {
			partitionFilePath = storageDirPath + arrayFolder + ".txt";
		} else {
			partitionFilePath = storageDirPath + File.separator + arrayFolder + ".txt";
		}
		
		try {
			storageHandler.deleteIfExists(partitionFilePath);
		} catch (Exception e) {
			throw new IOException(e);
		}

		logs.debug("deletePartitionIdFile(...) Partition Id file deleted : {}", partitionFilePath);
		
		return 0;
	}
	
	private int deletePartitionIdFile() throws Exception {
		
		String partitionFilePath = null;
		if (m_storageDirPath.endsWith(File.separator)) {
			partitionFilePath = m_storageDirPath + m_arrayFolder + ".txt";
		} else {
			partitionFilePath = m_storageDirPath + File.separator + m_arrayFolder + ".txt";
		}
		
		try {
			m_storageHandler.deleteIfExists(partitionFilePath);
		} catch (Exception e) {
			throw new IOException(e);
		}
		
		log.debug("deletePartitionIdFile() Partition Id file deleted : {}", partitionFilePath);
		
		return 0;
	}
	
	void setOperationTypeAppend() {
		m_operationType = 0;
	}
	
	void setOperationTypeGet() throws IOException {
		if (0 == m_operationType && false == m_isDirectorySupported) {
			flushFiles();
		}
		m_operationType = 1;
	}
	
	public void flushFiles() throws IOException {
		// System.out.println("flushFiles() Flushing files for data partition : " + m_arrayFolder + "...");
		log.debug("flushFiles() Flushing files for data partition : {}...", m_arrayFolder);
		m_bigArray.flushFiles();
		// System.out.println("flushFiles() Flushing files for data partition : " + m_arrayFolder + " done");
		log.debug("flushFiles() Flushing files for data partition : {} done", m_arrayFolder);
	}
	
	public IBigArray getBigArray() {
		return m_bigArray;
	}
	
	public long getLastIndex() {
		return m_lastIndex;
	}
	
    public Iterator<DataElement> iterator() {
        return new DataPartitionIterator(this);
    }	
	
	private void gc() {
		m_gcCounter.inc();
		if (m_gcCounter.value() >= 100000) {
			System.gc();
			m_gcCounter.set(0);
		}
	}	
	
	public static boolean isPartitionExists(String appId, String dataId, long splitUnit, String storageDirPath, IStorageHandler storageHandler) throws DScabiException, IOException {	

		String arrayFolder = dataId + "_" + splitUnit + "_" + appId.replace("_", "");

		boolean ret = isPartitionExists(storageDirPath, arrayFolder, storageHandler);
	
		return ret;
	}
	
	private static boolean isPartitionExists(String storageDirPath, String arrayFolder, IStorageHandler storageHandler) throws DScabiException, IOException {

		// System.out.println("isPartitionExists(...) Checking data partition : " + arrayFolder + "...");
		logs.debug("isPartitionExists(...) Checking data partition : {}...", arrayFolder);
		
		if (null == storageDirPath)
			throw new DScabiException("Storage Dir Path is null", "DPN.IPE.1");
		if (null == arrayFolder)
			throw new DScabiException("Array Folder is null", "DPN.IPE.2");
		if (storageDirPath.length() == 0)
			throw new DScabiException("Storage Dir Path is empty string", "DPN.IPE.3");
		if (arrayFolder.length() == 0)
			throw new DScabiException("Array Folder is empty string", "DPN.IPE.4");
			
		String dirPath = storageDirPath;
		
		if (dirPath.endsWith(File.separator))
			dirPath = dirPath + arrayFolder;
		else
			dirPath = dirPath + File.separator + arrayFolder;
		logs.debug("isPartitionExists(...) dirPath : {}", dirPath);

		String partitionFilePath = dirPath + ".txt";
		boolean ret = false;
		try {
			ret = storageHandler.isFileExists(partitionFilePath);
		} catch (Exception e) {
			throw new IOException(e);
		}
		
		// System.out.println("isPartitionExists(...) data partition data exists check result ret : " + ret);
		logs.debug("isPartitionExists(...) data partition data exists check result ret : {}", ret);
		
		return ret;
		
		/* Previous works
		String metaDataPageFilePath = dirPath + File.separator + "meta_data" + File.separator + "page-0.dat";
		logs.debug("isPartitionExists(...) metaDataPageFilePath : {}", metaDataPageFilePath);
		
		boolean ret1 = false;
		try {
			ret1 = storageHandler.isFileExists(metaDataPageFilePath);
		} catch (Exception e) {
			throw new IOException(e);
		}
		
		System.out.println("isPartitionExists(...) data partition meta_data exists check result ret1 : " + ret1);

		String indexDataPageFilePath = dirPath + File.separator + "index" + File.separator + "page-0.dat";
		logs.debug("isPartitionExists(...) indexDataPageFilePath : {}", indexDataPageFilePath);
		
		boolean ret2 = false;
		try {
			ret2 = storageHandler.isFileExists(indexDataPageFilePath);
		} catch (Exception e) {
			throw new IOException(e);
		}

		System.out.println("isPartitionExists(...) data partition index exists check result ret2 : " + ret2);
	
		String dataDataPageFilePath = dirPath + File.separator + "data" + File.separator + "page-0.dat";
		logs.debug("isPartitionExists(...) dataDataPageFilePath : {}", dataDataPageFilePath);
		
		boolean ret3 = false;
		try {
			ret3 = storageHandler.isFileExists(dataDataPageFilePath);
		} catch (Exception e) {
			throw new IOException(e);
		}

		System.out.println("isPartitionExists(...) data partition data exists check result ret3 : " + ret3);
		
		boolean ret = true;
		if (false == ret1 && false == ret2 && false == ret3) {
			ret = false;
		}
		return ret;
		*/
	}

	public static int deletePartition(String appId, String dataId, long splitUnit, String storageDirPath, IStorageHandler storageHandler) throws DScabiException, IOException {	
		String arrayFolder = dataId + "_" + splitUnit + "_" + appId.replace("_", "");

		int ret = deletePartition(storageDirPath, arrayFolder, storageHandler);
	
		return ret;
	}
	
	public static int deletePartition(String storageDirPath, String arrayFolder, IStorageHandler storageHandler) throws DScabiException, IOException {
		// NOTE: .close() or flushFiles() on that particular partition should be called by User before deleting arrayFolder 
		// of that partition using this static method
		
		// System.out.println("deletePartition(...) Deleting data partition : " + arrayFolder + "...");
		logs.debug("deletePartition(...) Deleting data partition : {}...", arrayFolder);
		
		if (null == storageDirPath)
			throw new DScabiException("Storage Dir Path is null", "DPN.DEN.1");
		if (null == arrayFolder)
			throw new DScabiException("Array Folder is null", "DPN.DEN.2");
		if (storageDirPath.length() == 0)
			throw new DScabiException("Storage Dir Path is empty string", "DPN.DEN.3");
		if (arrayFolder.length() == 0)
			throw new DScabiException("Array Folder is empty string", "DPN.DEN.4");
			
		String dirPath = storageDirPath;
		
		if (dirPath.charAt(dirPath.length() - 1) == File.separatorChar)
			dirPath = dirPath + arrayFolder;
		else
			dirPath = dirPath + File.separator + arrayFolder;
		logs.debug("deletePartition(...) dirPath : {}", dirPath);

		try {
			storageHandler.deleteDirIfExists(dirPath);
			deletePartitionIdFile(storageDirPath, arrayFolder, storageHandler);
		} catch (Exception e) {
			throw new IOException(e);
		}
		
		// System.out.println("deletePartition(...) Deleting data partition : " + arrayFolder + " done");
		logs.debug("deletePartition(...) Deleting data partition : {} done", arrayFolder);
		
		return 0;
	}

	public int deletePartition() throws IOException {
		close();
		
		// System.out.println("deletePartition() Deleting data partition : " + m_arrayFolder + "...");
		log.debug("deletePartition() Deleting data partition : {}...", m_arrayFolder);
		
		String dirPath = m_storageDirPath;

		if (dirPath.charAt(dirPath.length() - 1) == File.separatorChar)
			dirPath = dirPath + m_arrayFolder;
		else
			dirPath = dirPath + File.separator + m_arrayFolder;
		log.debug("deletePartition() dirPath : {}", dirPath);

		try {
			m_storageHandler.deleteDirIfExists(dirPath);
			deletePartitionIdFile();
		} catch (Exception e) {
			throw new IOException(e);
		}
		
		// System.out.println("deletePartition() Deleting data partition : " + m_arrayFolder + " done");
		log.debug("deletePartition() Deleting data partition : {} done", m_arrayFolder);
		
		return 0;
	}	
	
	public DataPartition(DataContext c, String dataId, String partitionId, String storageDirPath, String arrayFolder, int pageSize, String localDirPath, IStorageHandler storageHandler) throws Exception {

		if (null == c)
			throw new DScabiException("Data Context is null", "DPN.DPN.1");
		if (null == dataId)
			throw new DScabiException("Data Id is null", "DPN.DPN.2");
		if (null == partitionId)
			throw new DScabiException("Partition Id is null", "DPN.DPN.3");
		if (null == storageDirPath)
			throw new DScabiException("Storage Dir Path is null", "DPN.DPN.4");
		if (null == arrayFolder)
			throw new DScabiException("Array Folder is null", "DPN.DPN.5");
		if (pageSize <= 0)
			throw new DScabiException("Page Size <= 0", "DPN.DPN.6");
		if (null == localDirPath)
			throw new DScabiException("Local Dir Path is null", "DPN.DPN.4");
		if (storageDirPath.length() == 0)
			throw new DScabiException("Storage Dir Path is empty string", "DPN.DPN.7");
		if (arrayFolder.length() == 0)
			throw new DScabiException("Array Folder is empty string", "DPN.DPN.8");
		if (localDirPath.length() == 0)
			throw new DScabiException("Local Dir Path is empty string", "DPN.DPN.7");
		
		try {
			m_context = c;
			m_storageDirPath = storageDirPath;
			m_arrayFolder = arrayFolder;
			m_localDirPath = localDirPath;
			m_storageHandler = storageHandler;
			m_bigArray = new BigArrayImpl(storageDirPath, arrayFolder, pageSize, localDirPath, m_storageHandler);
			if (null == m_bigArray) {
				throw new DScabiException("big array is null", "DPN.DPN.10");
			} 
			
			if (m_bigArray.size() > 0) {
				m_lastIndex = m_bigArray.size() - 1;
			}
			
			if (0 == m_bigArray.size()) {
				createPartitionIdFile(storageDirPath, arrayFolder, localDirPath, storageHandler);
			}
			
			m_dataId = dataId;
			m_partitionId = partitionId;
			m_pageSize = pageSize;
			m_dson = new Dson();
			m_fieldsDson = new Dson();
			m_dataElement = new DataElement();
			
			// for zip purposes
		    m_forzip_zipbyteostream = new ByteArrayOutputStream();
		    m_forzip_gzipostream = new GZIPOutputStream(m_forzip_zipbyteostream);
		    // for unzip purposes
		    m_forunzip_unzipbyteostream = new ByteArrayOutputStream();
		    m_forunzip_unzipbuffer = new byte[1024];

		} catch (Exception e) {
				e.printStackTrace();
				throw e;
		}
	}
	
	private void initializeStreams() throws IOException {
		// for zip purposes
	    m_forzip_zipbyteostream = new ByteArrayOutputStream();
	    m_forzip_gzipostream = new GZIPOutputStream(m_forzip_zipbyteostream);
	    // for unzip purposes
	    m_forunzip_unzipbyteostream = new ByteArrayOutputStream();
	    m_forunzip_unzipbuffer = new byte[1024];
	}
	
	public void close() throws IOException {
		if (m_bigArray != null) {
			flushFiles();
			m_bigArray.close();
			m_bigArray = null;
		} 
		
		if (m_forzip_gzipostream != null) {
			m_forzip_gzipostream.close();
			m_forzip_gzipostream = null;
		}
		
		if (m_forzip_zipbyteostream != null) {
			m_forzip_zipbyteostream.close();
			m_forzip_zipbyteostream = null;
		}
			
		if (m_forunzip_unzipbyteostream != null) {
			m_forunzip_unzipbyteostream.close();
			m_forunzip_unzipbyteostream = null;
		}
	}
	
	private void closeArrayOnly() throws IOException {
		if (m_bigArray != null) {
			flushFiles();
			m_bigArray.close();
			m_bigArray = null;
		} 
	}
	
	public int shuffleBy(IShuffle g) {
		m_shuffle = g;
		return 0;
	}

	public int clearShuffle() {
		m_shuffle = null;
		return 0;
	}
	
	public int setZip(boolean doZip) {
		m_enableZip = doZip;
		return 0;
	}
	
	private byte[] zip(byte[] bytea) throws IOException {

	    m_forzip_zipbyteostream.reset();
	    // no reset() available for m_forzip_gzipostream
        m_forzip_gzipostream.write(bytea);
	    m_forzip_gzipostream.flush();
	    byte[] zipBytes = m_forzip_zipbyteostream.toByteArray();

	    return zipBytes;
	}
	
	private byte[] unzip(byte[] bytea) throws IOException {
		ByteArrayInputStream forunzip_zipbyteistream = new ByteArrayInputStream(bytea);
		GZIPInputStream forunzip_gzipistream = new GZIPInputStream(forunzip_zipbyteistream);

	    m_forunzip_unzipbyteostream.reset();
	    
        int len = 0;
        while ((len = forunzip_gzipistream.read(m_forunzip_unzipbuffer)) > 0) {
        	m_forunzip_unzipbyteostream.write(m_forunzip_unzipbuffer, 0, len);
        	m_forunzip_unzipbyteostream.flush();
        }
        
	    byte[] unzipBytes = m_forunzip_unzipbyteostream.toByteArray();

	    forunzip_gzipistream.close();
	    forunzip_zipbyteistream.close();
	    
	    return unzipBytes;
	}
	
	// hash(...) and related methods
	
	private long hash(Iterable<String> values) {
		long hash = 0;

		for (String s : values) {
			long hashValue = DMUtil.hashString(s); // alternatively use s.hashCode() but this is limited to int range only
			if (hashValue < 0)
				hashValue = -1 * hashValue;
			hash = hash + hashValue;
		}
		
		// if addition of hashes overflows long's max value, it will become negative. So make it positive
		if (hash < 0)
			hash = -1 * hash;
		
		return hash;
	}
	
	public long hashDataElement(long index) throws Exception {
		gc();
		byte[] bytea = m_bigArray.get(index);
		m_dataElement.set(bytea);
		Iterable<String> values = m_shuffle.groupValues(m_dataElement, m_context);
		long hash = hash(values);
		return hash;
	}

	public long hashCurrentDataElement() throws Exception {
		if (m_currentIndex > m_lastIndex)
			throw new DScabiException("m_currentIndex is not valid", "DPN.HDE.1");
		else if (m_currentIndex < 0)
			throw new DScabiException("m_currentIndex is not valid", "DPN.HDE.2");
		
		gc();
		byte[] bytea = m_bigArray.get(m_currentIndex);
		m_dataElement.set(bytea);
		Iterable<String> values = m_shuffle.groupValues(m_dataElement, m_context);
		long hash = hash(values);
		return hash;
	}
	
	public boolean isHashElementBelongsToSU(long index, long tu, long su) throws Exception {
		gc();
		byte[] bytea = m_bigArray.get(index);
		m_dataElement.set(bytea);
		Iterable<String> values = m_shuffle.groupValues(m_dataElement, m_context);
		long hash = hash(values);
		
		if (hash >= 0) {
			if ((hash % tu) + 1 == su)
				return true;
			else
				return false;
		} else {
			long n = (-1 * hash) % tu;
			if (n + 1 == su)
				return true;
			else
				return false;
		}
	}
	
	public boolean isHashCurrentElementBelongsToSU(long tu, long su) throws Exception {
		if (m_currentIndex > m_lastIndex)
			throw new DScabiException("m_currentIndex is not valid", "DPN.IHB.1");
		else if (m_currentIndex < 0)
			throw new DScabiException("m_currentIndex is not valid", "DPN.IHB.2");
		
		gc();
		byte[] bytea = m_bigArray.get(m_currentIndex);
		m_dataElement.set(bytea);
		Iterable<String> values = m_shuffle.groupValues(m_dataElement, m_context);
		long hash = hash(values);
		
		if (hash >= 0) {
			if ((hash % tu) + 1 == su)
				return true;
			else
				return false;
		} else {
			long n = (-1 * hash) % tu;
			if (n + 1 == su)
				return true;
			else
				return false;
		}
	}	
	
	// next...(...) and related methods
	
	public int begin() {
		m_currentIndex = -1;
		return 0;
	}
	public boolean hasNext() {
		if (m_currentIndex + 1 <= m_lastIndex)
			return true;
		else
			return false;
	}
	
	public byte[] nextBytes() throws IOException {

		gc();
		setOperationTypeGet();		
		
		if (m_currentIndex + 1 <= m_lastIndex) {
			byte[] bytea = m_bigArray.get(m_currentIndex + 1);
			m_currentIndex++;
			return bytea;
		}	
		else
			return null;
	}
	
	public String nextString() throws IOException {

		gc();
		setOperationTypeGet();		
		
		if (m_currentIndex + 1 <= m_lastIndex) {
			byte[] bytea = m_bigArray.get(m_currentIndex + 1);
			String s = new String(bytea, M_DEFAULT_ENCODING);
			m_currentIndex++;
			return s;
		}	
		else
			return null;
	}
	
	public String nextUnicodeString() throws IOException {

		gc();
		setOperationTypeGet();		
		
		if (m_currentIndex + 1 <= m_lastIndex) {
			byte[] bytea = m_bigArray.get(m_currentIndex + 1);
			String s = new String(bytea);
			m_currentIndex++;
			return s;
		}	
		else
			return null;
	}
	
	public DataElement next() throws IOException, DScabiException {

		gc();
		setOperationTypeGet();		
		
		if (m_currentIndex + 1 <= m_lastIndex) {
			byte[] bytea = m_bigArray.get(m_currentIndex + 1);
			DataElement e = new DataElement(bytea);
			m_currentIndex++;
			return e;
		}	
		else
			return null;
	}

	public int next(DataElement e) throws IOException, DScabiException {

		gc();
		setOperationTypeGet();		
		
		if (m_currentIndex + 1 <= m_lastIndex) {
			byte[] bytea = m_bigArray.get(m_currentIndex + 1);
			e.set(bytea);
			m_currentIndex++;
			return 0;
		}	
		else
			return -1;
	}	
	
	public Dson nextDson() throws IOException {

		gc();
		setOperationTypeGet();		
		
		if (m_currentIndex + 1 <= m_lastIndex) {
			byte[] bytea = m_bigArray.get(m_currentIndex + 1);
			String s = new String(bytea, M_DEFAULT_ENCODING);
			// TODO further analysis needed. Do I return same m_dson or new Dson every time?
			Dson dson = new Dson(s);
			m_currentIndex++;
			return dson;
		}	
		else
			return null;
	}
	
	public int nextDson(Dson dson) throws IOException {

		gc();
		setOperationTypeGet();		
		
		if (m_currentIndex + 1 <= m_lastIndex) {
			byte[] bytea = m_bigArray.get(m_currentIndex + 1);
			String s = new String(bytea, M_DEFAULT_ENCODING);
			dson.set(s);
			m_currentIndex++;
			return 0;
		}	
		else
			return -1;
	}
	
	public <T> T next(Class<T> t) throws IOException {

		gc();
		setOperationTypeGet();		
		
		if (m_currentIndex + 1 <= m_lastIndex) {
			byte[] bytea = m_bigArray.get(m_currentIndex + 1);
			T tobj = m_objectMapper.readValue(bytea, t);
			m_currentIndex++;
			return tobj;
		}	
		else
			return null;
	}
	
	public int next(IDsonInput d) throws Exception {

		gc();
		setOperationTypeGet();		
		
		if (m_currentIndex + 1 <= m_lastIndex) {
			byte[] bytea = m_bigArray.get(m_currentIndex + 1);
			String s = new String(bytea, M_DEFAULT_ENCODING);
			m_dson.set(s);
			d.set(m_dson);
			m_currentIndex++;
			return 0;
		}	
		else
			return -1;
	}

	// next...() for primary data types
	
	public int nextInt() throws IOException, DScabiException {

		gc();
		setOperationTypeGet();		
		
		if (m_currentIndex + 1 <= m_lastIndex) {
			byte[] bytea = m_bigArray.get(m_currentIndex + 1);
			String s = new String(bytea, M_DEFAULT_ENCODING);
			int x = Integer.parseInt(s);
			m_currentIndex++;
			return x;
		}	
		else
			throw new DScabiException("DataPartition last index is reached", "DPN.NIT.1");
	}
	
	public long nextLong() throws IOException, DScabiException {

		gc();
		setOperationTypeGet();		
		
		if (m_currentIndex + 1 <= m_lastIndex) {
			byte[] bytea = m_bigArray.get(m_currentIndex + 1);
			String s = new String(bytea, M_DEFAULT_ENCODING);
			long x = Long.parseLong(s);
			m_currentIndex++;
			return x;
		}	
		else
			throw new DScabiException("DataPartition last index is reached", "DPN.NLG.1");
	}
	
	public float nextFloat() throws IOException, DScabiException {

		gc();
		setOperationTypeGet();		
		
		if (m_currentIndex + 1 <= m_lastIndex) {
			byte[] bytea = m_bigArray.get(m_currentIndex + 1);
			String s = new String(bytea, M_DEFAULT_ENCODING);
			float x = Float.parseFloat(s);
			m_currentIndex++;
			return x;
		}	
		else
			throw new DScabiException("DataPartition last index is reached", "DPN.NFT.1");
	}
	
	public double nextDouble() throws IOException, DScabiException {

		gc();
		setOperationTypeGet();		
		
		if (m_currentIndex + 1 <= m_lastIndex) {
			byte[] bytea = m_bigArray.get(m_currentIndex + 1);
			String s = new String(bytea, M_DEFAULT_ENCODING);
			double x = Double.parseDouble(s);
			m_currentIndex++;
			return x;
		}	
		else
			throw new DScabiException("DataPartition last index is reached", "DPN.NDE.1");
	}
	
	public boolean nextBoolean() throws IOException, DScabiException {

		gc();
		setOperationTypeGet();		
		
		if (m_currentIndex + 1 <= m_lastIndex) {
			byte[] bytea = m_bigArray.get(m_currentIndex + 1);
			String s = new String(bytea, M_DEFAULT_ENCODING);
			boolean x = Boolean.parseBoolean(s);
			m_currentIndex++;
			return x;
		}	
		else
			throw new DScabiException("DataPartition last index is reached", "DPN.NBN.1");
	}
	
	// get...(...) methods

	public byte[] getBytes(long index) throws IOException {

		gc();
		setOperationTypeGet();		
		
		byte[] bytea = m_bigArray.get(index);
		return bytea;
	}
	
	public String getString(long index) throws IOException {

		gc();
		setOperationTypeGet();		
		
		byte[] bytea = m_bigArray.get(index);
		return new String(bytea, M_DEFAULT_ENCODING);
	}
	
	public DataElement get(long index) throws IOException, DScabiException {

		gc();
		setOperationTypeGet();		
		
		byte[] bytea = m_bigArray.get(index);
		return new DataElement(bytea);
	}

	public byte[] getBytes() throws IOException {

		gc();
		setOperationTypeGet();		
		
		if (m_currentIndex <= m_lastIndex) {
			byte[] bytea = m_bigArray.get(m_currentIndex);
			return bytea;
		}	
		else
			return null;
	}
	
	public String getString() throws IOException {

		gc();
		setOperationTypeGet();		
		
		if (m_currentIndex <= m_lastIndex) {
			byte[] bytea = m_bigArray.get(m_currentIndex);
			String s = new String(bytea, M_DEFAULT_ENCODING);
			return s;
		}	
		else
			return null;
	}

	public String getUnicodeString() throws IOException {

		gc();
		setOperationTypeGet();		
		
		if (m_currentIndex <= m_lastIndex) {
			byte[] bytea = m_bigArray.get(m_currentIndex);
			String s = new String(bytea);
			return s;
		}	
		else
			return null;
	}

	public DataElement get() throws IOException, DScabiException {

		gc();
		setOperationTypeGet();		
		
		if (m_currentIndex <= m_lastIndex) {
			byte[] bytea = m_bigArray.get(m_currentIndex);
			// TODO further analysis needed. Do I return same m_dataElement or new DataElement object every time?
			DataElement e = new DataElement(bytea);
			return e;
		}	
		else
			return null;
	}
	
	public int get(DataElement e) throws IOException, DScabiException {

		gc();
		setOperationTypeGet();		
		
		if (m_currentIndex <= m_lastIndex) {
			byte[] bytea = m_bigArray.get(m_currentIndex);
			e.set(bytea);
			return 0;
		}	
		else
			return -1;
	}	
	
	public Dson getDson() throws IOException {

		gc(); // gc() call is needed because if this getDson() method is called many times, many Dson objects will be created 
		setOperationTypeGet();
		
		if (m_currentIndex <= m_lastIndex) {
			byte[] bytea = m_bigArray.get(m_currentIndex);
			String s = new String(bytea, M_DEFAULT_ENCODING);
			// TODO further analysis needed. Use m_dson or create new Dson every time?
			Dson dson = new Dson(s);
			return dson;
		}	
		else
			return null;
	}
	
	public int getDson(Dson dson) throws IOException {

		gc();
		setOperationTypeGet();
		
		if (m_currentIndex <= m_lastIndex) {
			byte[] bytea = m_bigArray.get(m_currentIndex);
			String s = new String(bytea, M_DEFAULT_ENCODING);
			dson.set(s);
			return 0;
		}	
		else
			return -1;
	}
	
	public <T> T get(Class<T> t) throws IOException {
		
		gc();
		setOperationTypeGet();
		
		if (m_currentIndex <= m_lastIndex) {
			byte[] bytea = m_bigArray.get(m_currentIndex);
			T tobj = m_objectMapper.readValue(bytea, t);
			return tobj;
		}	
		else
			return null;
	}
	
	public int get(IDsonInput d) throws Exception {
		
		gc();
		setOperationTypeGet();
		
		if (m_currentIndex <= m_lastIndex) {
			byte[] bytea = m_bigArray.get(m_currentIndex);
			String s = new String(bytea, M_DEFAULT_ENCODING);
			m_dson.set(s);
			d.set(m_dson);
			return 0;
		}	
		else
			return -1;
	}

	// get...() for primary data types
	
	public int getInt() throws IOException, DScabiException {
		
		gc();
		setOperationTypeGet();
		
		if (m_currentIndex <= m_lastIndex) {
			byte[] bytea = m_bigArray.get(m_currentIndex);
			String s = new String(bytea, M_DEFAULT_ENCODING);
			int x = Integer.parseInt(s);
			return x;
		}	
		else
			throw new DScabiException("DataPartition invalid current index", "DPN.GIT.1");
	}	
	
	public long getLong() throws IOException, DScabiException {
		
		gc();
		setOperationTypeGet();
		
		if (m_currentIndex <= m_lastIndex) {
			byte[] bytea = m_bigArray.get(m_currentIndex);
			String s = new String(bytea, M_DEFAULT_ENCODING);
			long x = Long.parseLong(s);
			return x;
		}	
		else
			throw new DScabiException("DataPartition invalid current index", "DPN.GLG.1");
	}	
	
	public float getFloat() throws IOException, DScabiException {
		
		gc();
		setOperationTypeGet();
		
		if (m_currentIndex <= m_lastIndex) {
			byte[] bytea = m_bigArray.get(m_currentIndex);
			String s = new String(bytea, M_DEFAULT_ENCODING);
			float x = Float.parseFloat(s);
			return x;
		}	
		else
			throw new DScabiException("DataPartition invalid current index", "DPN.GFT.1");
	}	
	
	public double getDouble() throws IOException, DScabiException {
		
		gc();
		setOperationTypeGet();
		
		if (m_currentIndex <= m_lastIndex) {
			byte[] bytea = m_bigArray.get(m_currentIndex);
			String s = new String(bytea, M_DEFAULT_ENCODING);
			double x = Double.parseDouble(s);
			return x;
		}	
		else
			throw new DScabiException("DataPartition invalid current index", "DPN.GDE.1");
	}	
	
	public boolean getBoolean() throws IOException, DScabiException {
		
		gc();
		setOperationTypeGet();
		
		if (m_currentIndex <= m_lastIndex) {
			byte[] bytea = m_bigArray.get(m_currentIndex);
			String s = new String(bytea, M_DEFAULT_ENCODING);
			boolean x = Boolean.parseBoolean(s);
			return x;
		}	
		else
			throw new DScabiException("DataPartition invalid current index", "DPN.GBN.1");
	}		
	
	// get...Field(...) methods for fields
	
	public String getField(String fieldName) throws IOException {
		
		gc(); // gc() call is needed because if this get...Field() method is called many times, many String/byte[] objects will be created 
		setOperationTypeGet();
		
		if (m_currentIndex <= m_lastIndex) {
			byte[] bytea = m_bigArray.get(m_currentIndex);
			String s = new String(bytea, M_DEFAULT_ENCODING);
			m_dson.set(s);
			String s2 = m_dson.getString(fieldName);
			return s2;
		}	
		else
			return null;
	}

	public int getIntField(String fieldName) throws IOException, DScabiException {
		
		gc(); // gc() call is needed because if this get...Field() method is called many times, many String/byte[] objects will be created 
		setOperationTypeGet();
		
		if (m_currentIndex <= m_lastIndex) {
			byte[] bytea = m_bigArray.get(m_currentIndex);
			String s = new String(bytea, M_DEFAULT_ENCODING);
			m_dson.set(s);
			int s2 = m_dson.getInt(fieldName);
			return s2;
		}	
		else
			throw new DScabiException("invalid current index", "DPN.GIT.1");
	}
	
	public long getLongField(String fieldName) throws IOException, DScabiException {
		
		gc(); // gc() call is needed because if this get...Field() method is called many times, many String/byte[] objects will be created 
		setOperationTypeGet();
		
		if (m_currentIndex <= m_lastIndex) {
			byte[] bytea = m_bigArray.get(m_currentIndex);
			String s = new String(bytea, M_DEFAULT_ENCODING);
			m_dson.set(s);
			long s2 = m_dson.getLong(fieldName);
			return s2;
		}	
		else
			throw new DScabiException("invalid current index", "DPN.GLG.1");
	}
	
	public double getDoubleField(String fieldName) throws IOException, DScabiException {
		
		gc(); // gc() call is needed because if this get...Field() method is called many times, many String/byte[] objects will be created 
		setOperationTypeGet();
		
		if (m_currentIndex <= m_lastIndex) {
			byte[] bytea = m_bigArray.get(m_currentIndex);
			String s = new String(bytea, M_DEFAULT_ENCODING);
			m_dson.set(s);
			double s2 = m_dson.getDouble(fieldName);
			return s2;
		}	
		else
			throw new DScabiException("invalid current index", "DPN.GDE.1");
	}
	
	public boolean getBooleanField(String fieldName) throws IOException, DScabiException {
		
		gc(); // gc() call is needed because if this get...Field() method is called many times, many String/byte[] objects will be created 
		setOperationTypeGet();
		
		if (m_currentIndex <= m_lastIndex) {
			byte[] bytea = m_bigArray.get(m_currentIndex);
			String s = new String(bytea, M_DEFAULT_ENCODING);
			m_dson.set(s);
			boolean s2 = m_dson.getBoolean(fieldName);
			return s2;
		}	
		else
			throw new DScabiException("invalid current index", "DPN.GBN.1");
	}

	// Data class' foreachElement() method will basically call this method on the ComputeServer side
	public int foreachElement(IForEachDataElement fe) throws Exception {
		
		gc();
		
		if (m_lastIndex < 0)
			throw new DScabiException("m_lastIndex is not valid", "DPN.FEH.1");
		
		for (long i = 0; i <= m_lastIndex; i++) {
			gc();
			setOperationTypeGet();
			byte[] bytea = m_bigArray.get(i);
			m_dataElement.set(bytea);
			fe.call(m_dataElement, m_context);
		}
		
		return 0;
	}
	
	// Data class' foreachPartition() method will basically call this method on the ComputeServer side
	public int foreachPartition(IForEachDataPartition fe) throws Exception {
		
		gc(); // because if fe.call(...) does processing for each element in this partiton, better to do System.gc() after the fe.call(...)
		setOperationTypeGet();
		
		fe.call(this, m_context);
		
		return 0;
	}
	
	// append(...) methods
	
	// byte[], String, DataElement, Dson, <T> T tobj, 
	// (String keyName, String key, String valueName, String value), (String, String, String, int), (String, String, String, Integer), etc

	public void append(byte[] bytea) throws IOException {
		gc();
		setOperationTypeAppend();
		
		m_bigArray.append(bytea);
		m_lastIndex++;
	}
	
	public void append(String s) throws IOException {
		gc();
		setOperationTypeAppend();
		
		byte[] bytea = s.getBytes(M_DEFAULT_ENCODING); // so other 2-byte Unicode languages are not supported
		m_bigArray.append(bytea);	
		m_lastIndex++;
	}
	
	public void appendUnicode(String s) throws IOException {
		gc();
		setOperationTypeAppend();
		
		byte[] bytea = s.getBytes();
		m_bigArray.append(bytea);
		m_lastIndex++;
	}
	
	public void append(DataElement e) throws IOException {
		gc();
		setOperationTypeAppend();
		
		byte[] bytea = e.getBytes();
		m_bigArray.append(bytea);	
		m_lastIndex++;
	}
	
	public void append(Dson dson) throws IOException {
		gc();
		setOperationTypeAppend();
		
		byte[] bytea = dson.toString().getBytes(M_DEFAULT_ENCODING); // so other 2-byte Unicode languages are not supported
		m_bigArray.append(bytea);	
		m_lastIndex++;
	}
		
	public <T> T append(T tobj) throws IOException {
		gc();
		setOperationTypeAppend();
		
		byte[] bytea = m_objectMapper.writeValueAsBytes(tobj);
		m_bigArray.append(bytea);
		m_lastIndex++;
		
		return tobj;
	}
	
	// append(...) for primary data types and related
	
	public void append(int value) throws IOException {
		gc();
		setOperationTypeAppend();
		
		String s = "" + value;
		byte[] bytea = s.getBytes(M_DEFAULT_ENCODING); // so other 2-byte Unicode languages are not supported
		m_bigArray.append(bytea);	
		m_lastIndex++;
	}
	
	public void append(long value) throws IOException {
		gc();
		setOperationTypeAppend();
		
		String s = "" + value;
		byte[] bytea = s.getBytes(M_DEFAULT_ENCODING); // so other 2-byte Unicode languages are not supported
		m_bigArray.append(bytea);	
		m_lastIndex++;
	}
	
	public void append(float value) throws IOException {
		gc();
		setOperationTypeAppend();
		
		String s = "" + value;
		byte[] bytea = s.getBytes(M_DEFAULT_ENCODING); // so other 2-byte Unicode languages are not supported
		m_bigArray.append(bytea);	
		m_lastIndex++;
	}

	public void append(double value) throws IOException {
		gc();
		setOperationTypeAppend();
		
		String s = "" + value;
		byte[] bytea = s.getBytes(M_DEFAULT_ENCODING); // so other 2-byte Unicode languages are not supported
		m_bigArray.append(bytea);	
		m_lastIndex++;
	}

	public void append(boolean value) throws IOException {
		gc();
		setOperationTypeAppend();
		
		String s = "" + value;
		byte[] bytea = s.getBytes(M_DEFAULT_ENCODING); // so other 2-byte Unicode languages are not supported
		m_bigArray.append(bytea);	
		m_lastIndex++;
	}

	public void append(Integer value) throws IOException {
		gc();
		setOperationTypeAppend();
		
		String s = "" + value;
		byte[] bytea = s.getBytes(M_DEFAULT_ENCODING); // so other 2-byte Unicode languages are not supported
		m_bigArray.append(bytea);	
		m_lastIndex++;
	}

	public void append(Long value) throws IOException {
		gc();
		setOperationTypeAppend();
		
		String s = "" + value;
		byte[] bytea = s.getBytes(M_DEFAULT_ENCODING); // so other 2-byte Unicode languages are not supported
		m_bigArray.append(bytea);	
		m_lastIndex++;
	}

	public void append(Float value) throws IOException {
		gc();
		setOperationTypeAppend();
		
		String s = "" + value;
		byte[] bytea = s.getBytes(M_DEFAULT_ENCODING); // so other 2-byte Unicode languages are not supported
		m_bigArray.append(bytea);	
		m_lastIndex++;
	}

	public void append(Double value) throws IOException {
		gc();
		setOperationTypeAppend();
		
		String s = "" + value;
		byte[] bytea = s.getBytes(M_DEFAULT_ENCODING); // so other 2-byte Unicode languages are not supported
		m_bigArray.append(bytea);	
		m_lastIndex++;
	}

	public void append(Boolean value) throws IOException {
		gc();
		setOperationTypeAppend();
		
		String s = "" + value;
		byte[] bytea = s.getBytes(M_DEFAULT_ENCODING); // so other 2-byte Unicode languages are not supported
		m_bigArray.append(bytea);	
		m_lastIndex++;
	}

	// appendField(...) and appendRow() methods
	
	public int appendRow() throws IOException {
		gc();
		setOperationTypeAppend();
		
		byte[] bytea = m_fieldsDson.toString().getBytes(M_DEFAULT_ENCODING); // so other 2-byte Unicode languages are not supported
		m_bigArray.append(bytea);	
		m_lastIndex++;
		
		// m_fieldsDson is used only by appendField(...) and appendRow() methods
		m_fieldsDson.clear();
		
		return 0;
	}
	
	public void appendField(String fieldName, String fieldValue) throws IOException {
		m_fieldsDson.add(fieldName, fieldValue);
	}

	public void appendField(String fieldName, int fieldValue) throws IOException {
		m_fieldsDson.add(fieldName, fieldValue);
	}
	
	public void appendField(String fieldName, long fieldValue) throws IOException {
		m_fieldsDson.add(fieldName, fieldValue);
	}
	
	public void appendField(String fieldName, float fieldValue) throws IOException {
		m_fieldsDson.add(fieldName, fieldValue);
	}

	public void appendField(String fieldName, double fieldValue) throws IOException {
		m_fieldsDson.add(fieldName, fieldValue);
	}

	public void appendField(String fieldName, boolean fieldValue) throws IOException {
		m_fieldsDson.add(fieldName, fieldValue);
	}

	public void appendField(String fieldName, Integer fieldValue) throws IOException {
		m_fieldsDson.add(fieldName, fieldValue);
	}

	public void appendField(String fieldName, Long fieldValue) throws IOException {
		m_fieldsDson.add(fieldName, fieldValue);
	}
	
	public void appendField(String fieldName, Float fieldValue) throws IOException {
		m_fieldsDson.add(fieldName, fieldValue);
	}

	public void appendField(String fieldName, Double fieldValue) throws IOException {
		m_fieldsDson.add(fieldName, fieldValue);
	}

	public void appendField(String fieldName, Boolean fieldValue) throws IOException {
		m_fieldsDson.add(fieldName, fieldValue);
	}

	public void removeAll() throws IOException {
		// m_bigArray.removeAll() --> Don't use, not supported in new bigarray
		close();
		deletePartition();
		m_bigArray = new BigArrayImpl(m_storageDirPath, m_arrayFolder, m_pageSize, m_localDirPath, m_storageHandler);
		m_lastIndex = -1;
		m_currentIndex = -1;
		// Create zip, unzip streams as they are closed above by call to close() and deletePartition()
		initializeStreams();
	}
	
	/* not possible?
	public void remove(long index) {
		
		for (long i = index; i<= m_lastIndex - 1; i++) {
			m_bigArray.
		}
		
	}
	*/
	
	public long size() {
		return m_bigArray.size();
	}
	
	public int exportToFileForGivenSU(String filePath, long tu, long su) throws Exception {
		
		if (tu <= 0)
			throw new DScabiException("tu <= 0", "DPN.ETF.1");
		if (su <= 0)
			throw new DScabiException("su <= 0", "DPN.ETF.2");
		if (su > tu)
			throw new DScabiException("su > tu", "DPN.ETF.3");
		
		flushFiles();
		File f = new File(filePath);
		if (f.exists() == true)
			f.delete();
		f.createNewFile();
		
		if (m_lastIndex < 0) {
			return -1;
		}
		FileOutputStream fos = new FileOutputStream(filePath);
		OutputStreamWriter osw = new OutputStreamWriter(fos);
		BufferedWriter bw = new BufferedWriter(osw);
		
		byte[] bytea = null;
		String line = null;
		Iterable<String> values = null;
		long hash = 0;
		for (long i = 0; i <= m_lastIndex; i++) {
			gc();
			bytea = m_bigArray.get(i);
			
			// Determine hash
			m_dataElement.set(bytea);
			values = m_shuffle.groupValues(m_dataElement, m_context);
			hash = hash(values);
			if (hash >= 0) {
				if ((hash % tu) + 1 != su)
					continue;
			} else {
				long n = (-1 * hash) % tu;
				if (n + 1 != su)
					continue;
			}
			
			line = DMUtil.toHexString(bytea);
			line = line + "\n";
			log.debug("line : {}", line);
			bw.write(line);
		}
		bw.close();
		osw.close();
		fos.close();

		return 0;
	}	
	
	public int exportToFile(String filePath) throws IOException {
		flushFiles();
		File f = new File(filePath);
		if (f.exists() == true)
			f.delete();
		f.createNewFile();
		
		if (m_lastIndex < 0) {
			return -1;
		}
		FileOutputStream fos = new FileOutputStream(filePath);
		OutputStreamWriter osw = new OutputStreamWriter(fos);
		BufferedWriter bw = new BufferedWriter(osw);
		
		byte[] bytea = null;
		String line = null;
		for (long i = 0; i <= m_lastIndex; i++) {
			gc();
			bytea = m_bigArray.get(i);
			line = DMUtil.toHexString(bytea);
			line = line + "\n";
			log.debug("line : {}", line);
			bw.write(line);
		}
		bw.close();
		osw.close();
		fos.close();

		return 0;
	}
	
	public int importFromFile(String filePath) throws IOException {
		
		File f = new File(filePath);
		if (f.exists() == false)
			return -1;

		FileInputStream fis = new FileInputStream(filePath);
		InputStreamReader isr = new InputStreamReader(fis);
		BufferedReader br = new BufferedReader(isr);
		
		byte[] bytea = null;
		String line = null;
		// m_bigArray.removeAll() --> Don't use, not supported in new bigarray
		close();
		deletePartition();
		m_bigArray = new BigArrayImpl(m_storageDirPath, m_arrayFolder, m_pageSize, m_localDirPath, m_storageHandler);
		m_lastIndex = -1;
		m_currentIndex = -1;
		while ((line = br.readLine()) != null) {
			gc();
			log.debug("line : {}", line);
			log.debug("line.length() : {}", line.length());
			bytea = DMUtil.toBytesFromHexStr(line);
			m_bigArray.append(bytea);
			m_lastIndex++;
		}
		flushFiles();
		br.close();
		isr.close();
		fis.close();
		// Create zip, unzip streams as they are closed above by call to close() and deletePartition()
		initializeStreams();
		return 0;
	}	
	
	public String prettyPrint() throws UnsupportedEncodingException {
		
		String s = "";
		long index = 0;
		for (DataElement e : this) {
			gc();
			s = s + "[" + index + "]=>" + e.getString() + " ";
			index++;
		}
		s = s.substring(0, s.length() - 1);
		return s;
	}
}
