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
import java.time.Instant;
import java.util.Date;
import java.util.HashMap;
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
import com.dilmus.dilshad.scabi.common.DMJson;

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
	private String m_partitionUserRef = null;
	private int m_pageSize = 0;
	private String m_storageDirPath = null;
	private String m_arrayFolder = null;
	private String m_localDirPath = null;

	IStorageHandler m_storageHandler = null;
	
	private int m_operationType = -1; // -1 - not set, 0 - append operation, 1 - get operation
	private boolean m_isDirectorySupported = true; // does Storage system support directory creation given directory name
	
	private boolean m_isNew = false;
	private long m_retryNumber = -1;
	private String m_actualArrayFolder = null;
	private long m_parallelNumber = -1;
	
	private long m_totalNumOfBigArrays = 0;
	private HashMap<String, IBigArray> m_bigArrayMap = new HashMap<String, IBigArray>();
	private long m_currentBigArray = 0;
	private long m_lastBigArray = 0;
	private String m_createdBy = null;
	
	public static boolean isAppIdFileExists(String storageDirPath, String appId, IStorageHandler storageHandler) throws Exception {

		appId = appId.replace("_", "");
		// System.out.println("isAppIdFileExists(...) Checking App Id : " + appId + "...");
		logs.debug("isAppIdFileExists(...) Checking App Id : {}...", appId);
		
		if (null == storageDirPath)
			throw new DScabiException("Storage Dir Path is null", "DPN.IAE.1");
		if (null == appId)
			throw new DScabiException("App Id is null", "DPN.IAE.2");
		if (storageDirPath.length() == 0)
			throw new DScabiException("Storage Dir Path is empty string", "DPN.IAE.3");
		if (appId.length() == 0)
			throw new DScabiException("App Id is empty string", "DPN.IAE.4");
			
		String dirPath = storageDirPath;
		
		if (dirPath.endsWith(File.separator))
			dirPath = dirPath + appId;
		else
			dirPath = dirPath + File.separator + appId;
		logs.debug("isAppIdFileExists(...) dirPath : {}", dirPath);

		String appFilePath = dirPath + ".txt";
		boolean ret = false;
		ret = storageHandler.isFileExists(appFilePath);
		
		// System.out.println("isAppIdFileExists(...) App id file exists check result - ret : " + ret);
		logs.debug("isAppIdFileExists(...) App id file exists check result - ret : {}", ret);
		
		return ret;
	}
	
	public static String getAppIdFileData(String storageDirPath, String appId, String localDirPath, IStorageHandler storageHandler) throws Exception {
		
		appId = appId.replace("_", "");
		String localFilePath = null;
		if (localDirPath.endsWith(File.separator)) {
			localFilePath = localDirPath + appId + ".txt";
		} else {
			localFilePath = localDirPath + File.separator + appId + ".txt";
		}
		
		Path path = Paths.get(localFilePath);
		Files.deleteIfExists(path);
		
		String appFilePath = null;
		if (storageDirPath.endsWith(File.separator)) {
			appFilePath = storageDirPath + appId + ".txt";
		} else {
			appFilePath = storageDirPath + File.separator + appId + ".txt";
		}
		
		storageHandler.copyIfExistsToLocal(appFilePath, localFilePath);
		
		FileInputStream fis = new FileInputStream(localFilePath);
		InputStreamReader isr = new InputStreamReader(fis);
		BufferedReader br = new BufferedReader(isr);

		String line = br.readLine();
		
		br.close();
		isr.close();
		fis.close();
		
		Files.deleteIfExists(path);
		
		logs.debug("getAppIdFileData() App Id file data : {}", line);
		
		return line;		
		
	}
	
	public static int deleteLocalPartitionIfExists(String localPartitionDirPath) {
		
		logs.debug("deleteLocalPartitionIfExists() localPartitionDirPath : {}", localPartitionDirPath);
		
		File f = new File(localPartitionDirPath);
		if (f.exists()) {
			DMUtil.deleteFileDir(f);
		}
		return 0;
	}
	
	public void setDirectorySupported(boolean isDirectorySupported) {
		m_isDirectorySupported = isDirectorySupported;
	}
	
	// This is given default access
	static int createPartitionIdFile(String storageDirPath, String arrayFolder, String localDirPath, IStorageHandler storageHandler, String detailsStr) throws IOException {
		
		String localFilePath = null;
		if (localDirPath.endsWith(File.separator)) {
			localFilePath = localDirPath + arrayFolder + ".txt";
		} else {
			localFilePath = localDirPath + File.separator + arrayFolder + ".txt";
		}
		
		FileOutputStream fos = new FileOutputStream(localFilePath);
		OutputStreamWriter osw = new OutputStreamWriter(fos);
		BufferedWriter bw = new BufferedWriter(osw);

		// cw Date d = new Date();
		// cw bw.write(d.toString());
		bw.write(detailsStr);
		
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
		
		logs.debug("createPartitionIdFile() Partition Id file created : {}", partitionFilePath);
		
		return 0;
	}
	
	public static String getPartitionIdFileData(String storageDirPath, String arrayFolder, String localDirPath, IStorageHandler storageHandler) throws IOException {
		
		String localFilePath = null;
		if (localDirPath.endsWith(File.separator)) {
			localFilePath = localDirPath + arrayFolder + ".txt";
		} else {
			localFilePath = localDirPath + File.separator + arrayFolder + ".txt";
		}
		
		Path path = Paths.get(localFilePath);
		Files.deleteIfExists(path);
		
		String partitionFilePath = null;
		if (storageDirPath.endsWith(File.separator)) {
			partitionFilePath = storageDirPath + arrayFolder + ".txt";
		} else {
			partitionFilePath = storageDirPath + File.separator + arrayFolder + ".txt";
		}
		
		try {
			storageHandler.copyIfExistsToLocal(partitionFilePath, localFilePath);
		} catch (Exception e) {
			throw new IOException(e);
		}
		
		FileInputStream fis = new FileInputStream(localFilePath);
		InputStreamReader isr = new InputStreamReader(fis);
		BufferedReader br = new BufferedReader(isr);

		String line = br.readLine();
		
		br.close();
		isr.close();
		fis.close();
		
		Files.deleteIfExists(path);
		
		logs.debug("getPartitionIdFileData() Partition Id file data : {}", line);
		
		return line;		
		
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
	
	private int createPartitionIdRPCCFile(String storageDirPath, String arrayFolder, String localDirPath, IStorageHandler storageHandler, long retryNumber, long parallelNumber, String detailsStr) throws IOException {
		
		String localFilePath = null;
		if (localDirPath.endsWith(File.separator)) {
			localFilePath = localDirPath + arrayFolder + "_RPCC" + "_R" + retryNumber + "_P" + parallelNumber + ".txt";
		} else {
			localFilePath = localDirPath + File.separator + arrayFolder + "_RPCC" + "_R" + retryNumber + "_P" + parallelNumber + ".txt";
		}
		
		FileOutputStream fos = new FileOutputStream(localFilePath);
		OutputStreamWriter osw = new OutputStreamWriter(fos);
		BufferedWriter bw = new BufferedWriter(osw);

		// cw Date d = new Date();
		// cw bw.write(d.toString());
		bw.write(detailsStr);
		
		bw.close();
		osw.close();
		fos.close();
		
		String partitionFilePath = null;
		if (storageDirPath.endsWith(File.separator)) {
			partitionFilePath = storageDirPath + arrayFolder + "_RPCC" + "_R" + retryNumber + "_P" + parallelNumber + ".txt";
		} else {
			partitionFilePath = storageDirPath + File.separator + arrayFolder + "_RPCC" + "_R" + retryNumber + "_P" + parallelNumber + ".txt";
		}
		
		try {
			storageHandler.copyFromLocal(partitionFilePath, localFilePath);
		} catch (Exception e) {
			throw new IOException(e);
		}
		
		Path path = Paths.get(localFilePath);
		Files.deleteIfExists(path);
		
		log.debug("createPartitionIdRPCCFile() Partition Id RPCC file created : {}", partitionFilePath);
		
		return 0;
	}
	
	public static String getPartitionIdRPCCFileData(String storageDirPath, String arrayFolder, String localDirPath, IStorageHandler storageHandler, long retryNumber, long parallelNumber) throws IOException {
		
		String localFilePath = null;
		if (localDirPath.endsWith(File.separator)) {
			localFilePath = localDirPath + arrayFolder + "_RPCC" + "_R" + retryNumber + "_P" + parallelNumber +".txt";
		} else {
			localFilePath = localDirPath + File.separator + arrayFolder + "_RPCC" + "_R" + retryNumber + "_P" + parallelNumber+ ".txt";
		}
		
		Path path = Paths.get(localFilePath);
		Files.deleteIfExists(path);
		
		String partitionFilePath = null;
		if (storageDirPath.endsWith(File.separator)) {
			partitionFilePath = storageDirPath + arrayFolder + "_RPCC" + "_R" + retryNumber + "_P" + parallelNumber + ".txt";
		} else {
			partitionFilePath = storageDirPath + File.separator + arrayFolder + "_RPCC" + "_R" + retryNumber + "_P" + parallelNumber + ".txt";
		}
		
		try {
			storageHandler.copyIfExistsToLocal(partitionFilePath, localFilePath);
		} catch (Exception e) {
			throw new IOException(e);
		}
		
		FileInputStream fis = new FileInputStream(localFilePath);
		InputStreamReader isr = new InputStreamReader(fis);
		BufferedReader br = new BufferedReader(isr);

		String line = br.readLine();
		
		br.close();
		isr.close();
		fis.close();
		
		Files.deleteIfExists(path);
		
		logs.debug("getPartitionIdRPCCFileData() Partition Id RPCC file data : {}", line);
		
		return line;		
		
	}
	
	private static int deletePartitionIdRPCCFile(String storageDirPath, String arrayFolder, IStorageHandler storageHandler, long retryNumber, long parallelNumber) throws IOException {
		
		String partitionFilePath = null;
		if (storageDirPath.endsWith(File.separator)) {
			partitionFilePath = storageDirPath + arrayFolder + "_RPCC" + "_R" + retryNumber + "_P" + parallelNumber + ".txt";
		} else {
			partitionFilePath = storageDirPath + File.separator + arrayFolder + "_RPCC" + "_R" + retryNumber + "_P" + parallelNumber + ".txt";
		}
		
		try {
			storageHandler.deleteIfExists(partitionFilePath);
		} catch (Exception e) {
			throw new IOException(e);
		}

		logs.debug("deletePartitionIdRPCCFile(...) Partition Id RPCC file deleted : {}", partitionFilePath);
		
		return 0;
	}
	
	private int deletePartitionIdRPCCFile() throws Exception {
		
		String partitionFilePath = null;
		if (m_storageDirPath.endsWith(File.separator)) {
			partitionFilePath = m_storageDirPath + m_arrayFolder + "_RPCC" + "_R" + m_retryNumber + "_P" + m_parallelNumber + ".txt";
		} else {
			partitionFilePath = m_storageDirPath + File.separator + m_arrayFolder + "_RPCC" + "_R" + m_retryNumber + "_P" + m_parallelNumber + ".txt";
		}
		
		try {
			m_storageHandler.deleteIfExists(partitionFilePath);
		} catch (Exception e) {
			throw new IOException(e);
		}
		
		log.debug("deletePartitionIdRPCCFile() Partition Id RPCC file deleted : {}", partitionFilePath);
		
		return 0;
	}
	
	// This is given default access
	static int createPartitionIdDeletedFile(String storageDirPath, String arrayFolder, String localDirPath, IStorageHandler storageHandler, String detailsStr) throws IOException {
		
		String localFilePath = null;
		if (localDirPath.endsWith(File.separator)) {
			localFilePath = localDirPath + arrayFolder + "_Deleted.txt";
		} else {
			localFilePath = localDirPath + File.separator + arrayFolder + "_Deleted.txt";
		}
		
		FileOutputStream fos = new FileOutputStream(localFilePath);
		OutputStreamWriter osw = new OutputStreamWriter(fos);
		BufferedWriter bw = new BufferedWriter(osw);

		// cw Date d = new Date();
		// cw bw.write(d.toString());
		bw.write(detailsStr);
		
		bw.close();
		osw.close();
		fos.close();
		
		String partitionFilePath = null;
		if (storageDirPath.endsWith(File.separator)) {
			partitionFilePath = storageDirPath + arrayFolder + "_Deleted.txt";
		} else {
			partitionFilePath = storageDirPath + File.separator + arrayFolder + "_Deleted.txt";
		}
		
		try {
			storageHandler.copyFromLocal(partitionFilePath, localFilePath);
		} catch (Exception e) {
			throw new IOException(e);
		}
		
		Path path = Paths.get(localFilePath);
		Files.deleteIfExists(path);
		
		logs.debug("createPartitionIdDeletedFile() Partition Id_Deleted file created : {}", partitionFilePath);
		
		return 0;
	}	
	
	private static int deletePartitionIdDeletedFile(String storageDirPath, String arrayFolder, IStorageHandler storageHandler) throws IOException {
		
		String partitionFilePath = null;
		if (storageDirPath.endsWith(File.separator)) {
			partitionFilePath = storageDirPath + arrayFolder + "_Deleted.txt";
		} else {
			partitionFilePath = storageDirPath + File.separator + arrayFolder + "_Deleted.txt";
		}
		
		try {
			storageHandler.deleteIfExists(partitionFilePath);
		} catch (Exception e) {
			throw new IOException(e);
		}

		logs.debug("deletePartitionIdDeletedFile(...) Partition Id_Deleted file deleted : {}", partitionFilePath);
		
		return 0;
	}
	
	private int deletePartitionIdDeletedFile() throws Exception {
		
		String partitionFilePath = null;
		if (m_storageDirPath.endsWith(File.separator)) {
			partitionFilePath = m_storageDirPath + m_arrayFolder + "_Deleted.txt";
		} else {
			partitionFilePath = m_storageDirPath + File.separator + m_arrayFolder + "_Deleted.txt";
		}
		
		try {
			m_storageHandler.deleteIfExists(partitionFilePath);
		} catch (Exception e) {
			throw new IOException(e);
		}
		
		log.debug("deletePartitionIdDeletedFile() Partition Id_Deleted file deleted : {}", partitionFilePath);
		
		return 0;
	}
	
	private int createPartitionIdPRECNBAFile(String storageDirPath, String arrayFolder, String localDirPath, IStorageHandler storageHandler, long retryNumber, long parallelNumber, String detailsStr) throws IOException {
		
		String localFilePath = null;
		if (localDirPath.endsWith(File.separator)) {
			localFilePath = localDirPath + arrayFolder + "_PRECNBA" + "_R" + retryNumber + "_P" + parallelNumber + ".txt";
		} else {
			localFilePath = localDirPath + File.separator + arrayFolder + "_PRECNBA" + "_R" + retryNumber + "_P" + parallelNumber + ".txt";
		}
		
		FileOutputStream fos = new FileOutputStream(localFilePath);
		OutputStreamWriter osw = new OutputStreamWriter(fos);
		BufferedWriter bw = new BufferedWriter(osw);

		// cw Date d = new Date();
		// cw bw.write(d.toString());
		bw.write(detailsStr);
		
		bw.close();
		osw.close();
		fos.close();
		
		String partitionFilePath = null;
		if (storageDirPath.endsWith(File.separator)) {
			partitionFilePath = storageDirPath + arrayFolder + "_PRECNBA" + "_R" + retryNumber + "_P" + parallelNumber + ".txt";
		} else {
			partitionFilePath = storageDirPath + File.separator + arrayFolder + "_PRECNBA" + "_R" + retryNumber + "_P" + parallelNumber + ".txt";
		}
		
		try {
			storageHandler.copyFromLocal(partitionFilePath, localFilePath);
		} catch (Exception e) {
			throw new IOException(e);
		}
		
		Path path = Paths.get(localFilePath);
		Files.deleteIfExists(path);
		
		log.debug("createPartitionIdPRECNBAFile() Partition Id PRECNBA file created : {}", partitionFilePath);
		
		return 0;
	}
	
	private static int deletePartitionIdPRECNBAFile(String storageDirPath, String arrayFolder, IStorageHandler storageHandler, long retryNumber, long parallelNumber) throws IOException {
		
		String partitionFilePath = null;
		if (storageDirPath.endsWith(File.separator)) {
			partitionFilePath = storageDirPath + arrayFolder + "_PRECNBA" + "_R" + retryNumber + "_P" + parallelNumber + ".txt";
		} else {
			partitionFilePath = storageDirPath + File.separator + arrayFolder + "_PRECNBA" + "_R" + retryNumber + "_P" + parallelNumber + ".txt";
		}
		
		try {
			storageHandler.deleteIfExists(partitionFilePath);
		} catch (Exception e) {
			throw new IOException(e);
		}

		logs.debug("deletePartitionIdPRECNBAFile(...) Partition Id PRECNBA file deleted : {}", partitionFilePath);
		
		return 0;
	}
	
	private int deletePartitionIdPRECNBAFile() throws Exception {
		
		String partitionFilePath = null;
		if (m_storageDirPath.endsWith(File.separator)) {
			partitionFilePath = m_storageDirPath + m_arrayFolder + "_PRECNBA" + "_R" + m_retryNumber + "_P" + m_parallelNumber + ".txt";
		} else {
			partitionFilePath = m_storageDirPath + File.separator + m_arrayFolder + "_PRECNBA" + "_R" + m_retryNumber + "_P" + m_parallelNumber + ".txt";
		}
		
		try {
			m_storageHandler.deleteIfExists(partitionFilePath);
		} catch (Exception e) {
			throw new IOException(e);
		}
		
		log.debug("deletePartitionIdPRECNBAFile() Partition Id PRECNBA file deleted : {}", partitionFilePath);
		
		return 0;
	}
	
	private static boolean isPartitionIdPRECNBAFileExists(String storageDirPath, String arrayFolder, IStorageHandler storageHandler, long retryNumber, long parallelNumber) throws DScabiException, IOException {

		// System.out.println("isPartitionIdPRECNBAFileExists(...) Checking data partition : " + arrayFolder + "...");
		logs.debug("isPartitionIdPRECNBAFileExists(...) Checking data partition : {}...", arrayFolder);
		
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
		logs.debug("isPartitionIdPRECNBAFileExists(...) dirPath : {}", dirPath);

		String partitionFilePath = dirPath  + "_PRECNBA" + "_R" + retryNumber + "_P" + parallelNumber + ".txt";
		logs.debug("isPartitionIdPRECNBAFileExists(...) partition id PRECNBA file path : {}", partitionFilePath);
		
		boolean ret = false;
		try {
			ret = storageHandler.isFileExists(partitionFilePath);
		} catch (Exception e) {
			throw new IOException(e);
		}
		
		// System.out.println("isPartitionIdPRECNBAFileExists(...) data partition id PRECNBA file exists check result - ret : " + ret);
		logs.debug("isPartitionIdPRECNBAFileExists(...) data partition id PRECNBA file exists check result - ret : {}", ret);
		
		return ret;
	}	
	
	public static String getPartitionIdPRECNBAFileData(String storageDirPath, String arrayFolder, String localDirPath, IStorageHandler storageHandler, long retryNumber, long parallelNumber) throws IOException {
		
		String localFilePath = null;
		if (localDirPath.endsWith(File.separator)) {
			localFilePath = localDirPath + arrayFolder + "_PRECNBA" + "_R" + retryNumber + "_P" + parallelNumber + ".txt";
		} else {
			localFilePath = localDirPath + File.separator + arrayFolder + "_PRECNBA" + "_R" + retryNumber + "_P" + parallelNumber + ".txt";
		}
		
		Path path = Paths.get(localFilePath);
		Files.deleteIfExists(path);
		
		String partitionFilePath = null;
		if (storageDirPath.endsWith(File.separator)) {
			partitionFilePath = storageDirPath + arrayFolder + "_PRECNBA" + "_R" + retryNumber + "_P" + parallelNumber + ".txt";
		} else {
			partitionFilePath = storageDirPath + File.separator + arrayFolder + "_PRECNBA" + "_R" + retryNumber + "_P" + parallelNumber + ".txt";
		}
		
		try {
			storageHandler.copyIfExistsToLocal(partitionFilePath, localFilePath);
		} catch (Exception e) {
			throw new IOException(e);
		}
		
		FileInputStream fis = new FileInputStream(localFilePath);
		InputStreamReader isr = new InputStreamReader(fis);
		BufferedReader br = new BufferedReader(isr);

		String line = br.readLine();
		
		br.close();
		isr.close();
		fis.close();
		
		Files.deleteIfExists(path);
		
		logs.debug("getPartitionIdPRECNBAFileData() Partition Id PRECNBA file data : {}", line);
		
		return line;		
		
	}
	
	private int createPartitionIdPOSTCNBAFile(String storageDirPath, String arrayFolder, String localDirPath, IStorageHandler storageHandler, long retryNumber, long parallelNumber, String detailsStr) throws IOException {
		
		String localFilePath = null;
		if (localDirPath.endsWith(File.separator)) {
			localFilePath = localDirPath + arrayFolder + "_POSTCNBA" + "_R" + retryNumber + "_P" + parallelNumber + ".txt";
		} else {
			localFilePath = localDirPath + File.separator + arrayFolder + "_POSTCNBA" + "_R" + retryNumber + "_P" + parallelNumber + ".txt";
		}
		
		FileOutputStream fos = new FileOutputStream(localFilePath);
		OutputStreamWriter osw = new OutputStreamWriter(fos);
		BufferedWriter bw = new BufferedWriter(osw);

		// cw Date d = new Date();
		// cw bw.write(d.toString());
		bw.write(detailsStr);
		
		bw.close();
		osw.close();
		fos.close();
		
		String partitionFilePath = null;
		if (storageDirPath.endsWith(File.separator)) {
			partitionFilePath = storageDirPath + arrayFolder + "_POSTCNBA" + "_R" + retryNumber + "_P" + parallelNumber + ".txt";
		} else {
			partitionFilePath = storageDirPath + File.separator + arrayFolder + "_POSTCNBA" + "_R" + retryNumber + "_P" + parallelNumber + ".txt";
		}
		
		try {
			storageHandler.copyFromLocal(partitionFilePath, localFilePath);
		} catch (Exception e) {
			throw new IOException(e);
		}
		
		Path path = Paths.get(localFilePath);
		Files.deleteIfExists(path);
		
		log.debug("createPartitionIdPOSTCNBAFile() Partition Id POSTCNBA file created : {}", partitionFilePath);
		
		return 0;
	}
	
	private static int deletePartitionIdPOSTCNBAFile(String storageDirPath, String arrayFolder, IStorageHandler storageHandler, long retryNumber, long parallelNumber) throws IOException {
		
		String partitionFilePath = null;
		if (storageDirPath.endsWith(File.separator)) {
			partitionFilePath = storageDirPath + arrayFolder + "_POSTCNBA" + "_R" + retryNumber + "_P" + parallelNumber + ".txt";
		} else {
			partitionFilePath = storageDirPath + File.separator + arrayFolder + "_POSTCNBA" + "_R" + retryNumber + "_P" + parallelNumber + ".txt";
		}
		
		try {
			storageHandler.deleteIfExists(partitionFilePath);
		} catch (Exception e) {
			throw new IOException(e);
		}

		logs.debug("deletePartitionIdPOSTCNBAFile(...) Partition Id POSTCNBA file deleted : {}", partitionFilePath);
		
		return 0;
	}
	
	private int deletePartitionIdPOSTCNBAFile() throws Exception {
		
		String partitionFilePath = null;
		if (m_storageDirPath.endsWith(File.separator)) {
			partitionFilePath = m_storageDirPath + m_arrayFolder + "_POSTCNBA" + "_R" + m_retryNumber + "_P" + m_parallelNumber + ".txt";
		} else {
			partitionFilePath = m_storageDirPath + File.separator + m_arrayFolder + "_POSTCNBA" + "_R" + m_retryNumber + "_P" + m_parallelNumber + ".txt";
		}
		
		try {
			m_storageHandler.deleteIfExists(partitionFilePath);
		} catch (Exception e) {
			throw new IOException(e);
		}
		
		log.debug("deletePartitionIdPOSTCNBAFile() Partition Id POSTCNBA file deleted : {}", partitionFilePath);
		
		return 0;
	}
	
	private static boolean isPartitionIdPOSTCNBAFileExists(String storageDirPath, String arrayFolder, IStorageHandler storageHandler, long retryNumber, long parallelNumber) throws DScabiException, IOException {

		// System.out.println("isPartitionIdPOSTCNBAFileExists(...) Checking data partition : " + arrayFolder + "...");
		logs.debug("isPartitionIdPOSTCNBAFileExists(...) Checking data partition : {}...", arrayFolder);
		
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
		logs.debug("isPartitionIdPOSTCNBAFileExists(...) dirPath : {}", dirPath);

		String partitionFilePath = dirPath  + "_POSTCNBA" + "_R" + retryNumber + "_P" + parallelNumber + ".txt";
		logs.debug("isPartitionIdPOSTCNBAFileExists(...) partition id POSTCNBA file path : {}", partitionFilePath);
		
		boolean ret = false;
		try {
			ret = storageHandler.isFileExists(partitionFilePath);
		} catch (Exception e) {
			throw new IOException(e);
		}
		
		// System.out.println("isPartitionIdPOSTCNBAFileExists(...) data partition id POSTCNBA file exists check result - ret : " + ret);
		logs.debug("isPartitionIdPOSTCNBAFileExists(...) data partition id POSTCNBA file exists check result - ret : {}", ret);
		
		return ret;
	}	
	
	public static String getPartitionIdPOSTCNBAFileData(String storageDirPath, String arrayFolder, String localDirPath, IStorageHandler storageHandler, long retryNumber, long parallelNumber) throws IOException {
		
		String localFilePath = null;
		if (localDirPath.endsWith(File.separator)) {
			localFilePath = localDirPath + arrayFolder + "_POSTCNBA" + "_R" + retryNumber + "_P" + parallelNumber + ".txt";
		} else {
			localFilePath = localDirPath + File.separator + arrayFolder + "_POSTCNBA" + "_R" + retryNumber + "_P" + parallelNumber + ".txt";
		}
		
		Path path = Paths.get(localFilePath);
		Files.deleteIfExists(path);
		
		String partitionFilePath = null;
		if (storageDirPath.endsWith(File.separator)) {
			partitionFilePath = storageDirPath + arrayFolder + "_POSTCNBA" + "_R" + retryNumber + "_P" + parallelNumber + ".txt";
		} else {
			partitionFilePath = storageDirPath + File.separator + arrayFolder + "_POSTCNBA" + "_R" + retryNumber + "_P" + parallelNumber + ".txt";
		}
		
		try {
			storageHandler.copyIfExistsToLocal(partitionFilePath, localFilePath);
		} catch (Exception e) {
			throw new IOException(e);
		}
		
		FileInputStream fis = new FileInputStream(localFilePath);
		InputStreamReader isr = new InputStreamReader(fis);
		BufferedReader br = new BufferedReader(isr);

		String line = br.readLine();
		
		br.close();
		isr.close();
		fis.close();
		
		Files.deleteIfExists(path);
		
		logs.debug("getPartitionIdPOSTCNBAFileData() Partition Id POSTCNBA file data : {}", line);
		
		return line;		
		
	}
	
	
	void setOperationTypeAppend() {
		m_operationType = 0;
	}
	
	void setOperationTypeGet() throws Exception {
		if (0 == m_operationType && false == m_isDirectorySupported) {
			flushFiles();
		}
		m_operationType = 1;
	}
	
	public void flushFiles() throws Exception {
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
	
	public static boolean isPartitionIdFileExists(String appId, String dataId, long splitUnit, String storageDirPath, IStorageHandler storageHandler) throws DScabiException, IOException {	

		String arrayFolder = dataId + "_" + splitUnit + "_" + appId.replace("_", "");

		boolean ret = isPartitionIdFileExists(storageDirPath, arrayFolder, storageHandler);
	
		return ret;
	}
	
	private static boolean isPartitionIdFileExists(String storageDirPath, String arrayFolder, IStorageHandler storageHandler) throws DScabiException, IOException {

		// System.out.println("isPartitionIdFileExists(...) Checking data partition : " + arrayFolder + "...");
		logs.debug("isPartitionIdFileExists(...) Checking data partition : {}...", arrayFolder);
		
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
		logs.debug("isPartitionIdFileExists(...) dirPath : {}", dirPath);

		String partitionFilePath = dirPath + ".txt";
		boolean ret = false;
		try {
			ret = storageHandler.isFileExists(partitionFilePath);
		} catch (Exception e) {
			throw new IOException(e);
		}
		
		// System.out.println("isPartitionIdFileExists(...) data partition id file exists check result - ret : " + ret);
		logs.debug("isPartitionIdFileExists(...) data partition id file exists check result - ret : {}", ret);
		
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
	
	private static boolean isPartitionIdRPCCFileExists(String storageDirPath, String arrayFolder, IStorageHandler storageHandler, long retryNumber, long parallelNumber) throws DScabiException, IOException {

		// System.out.println("isPartitionIdRPCCFileExists(...) Checking data partition : " + arrayFolder + "...");
		logs.debug("isPartitionIdRPCCFileExists(...) Checking data partition : {}...", arrayFolder);
		
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
		logs.debug("isPartitionIdRPCCFileExists(...) dirPath : {}", dirPath);

		String partitionFilePath = dirPath  + "_RPCC" + "_R" + retryNumber + "_P" + parallelNumber + ".txt";
		logs.debug("isPartitionIdRPCCFileExists(...) partition id RPCC file path : {}", partitionFilePath);
		
		boolean ret = false;
		try {
			ret = storageHandler.isFileExists(partitionFilePath);
		} catch (Exception e) {
			throw new IOException(e);
		}
		
		// System.out.println("isPartitionIdRPCCFileExists(...) data partition id RPCC file exists check result - ret : " + ret);
		logs.debug("isPartitionIdRPCCFileExists(...) data partition id RPCC file exists check result - ret : {}", ret);
		
		return ret;
	}	
	
	private static boolean isPartitionIdDeletedFileExists(String storageDirPath, String arrayFolder, IStorageHandler storageHandler) throws DScabiException, IOException {

		// System.out.println("isPartitionIdDeletedFileExists(...) Checking data partition : " + arrayFolder + "...");
		logs.debug("isPartitionIdDeletedFileExists(...) Checking data partition : {}...", arrayFolder);
		
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
		logs.debug("isPartitionIdDeletedFileExists(...) dirPath : {}", dirPath);

		String partitionFilePath = dirPath  + "_Deleted.txt";
		logs.debug("isPartitionIdDeletedFileExists(...) partition id_Deleted file path : {}", partitionFilePath);
		
		boolean ret = false;
		try {
			ret = storageHandler.isFileExists(partitionFilePath);
		} catch (Exception e) {
			throw new IOException(e);
		}
		
		// System.out.println("isPartitionIdDeletedFileExists(...) data partition id_Deleted file exists check result - ret : " + ret);
		logs.debug("isPartitionIdDeletedFileExists(...) data partition id_Deleted file exists check result - ret : {}", ret);
		
		return ret;
	}	
	
	public static int deletePartition(String appId, String dataId, long splitUnit, String storageDirPath, String localDirPath, IStorageHandler storageHandler, String deletedBy) throws DScabiException, IOException {	
		String arrayFolder = dataId + "_" + splitUnit + "_" + appId.replace("_", "");

		int ret = deletePartition(storageDirPath, arrayFolder, localDirPath, storageHandler, deletedBy);
	
		return ret;
	}
	
	public static int deletePartition(String storageDirPath, String arrayFolder, String localDirPath, IStorageHandler storageHandler, String deletedBy) throws DScabiException, IOException {
		// NOTE: .close() or flushFiles() on that particular partition should be called by User before deleting arrayFolder 
		// of that partition using this static method
		
		// System.out.println("deletePartition(...) Deleting data partition : " + arrayFolder + "...");
		logs.debug("deletePartition(...) Deleting data partition : {}...", arrayFolder);
		long retryNumber = -1;
		long parallelNumber = -1;
		boolean proceed = false;
		long totalNumOfBigArrays = -1;
		DMJson djsonPid = null;
		
		String strAppId, strDataId, strSplitUnit, strPartitionUserRef, strArrayFolder;
		String strRetryNumber, strParallelNumber, strTotalNumOfBigArrays;
		String strMaxRetry, strMaxParallel;
		String strCreatedBy, strStatus = null;
		
		if (null == storageDirPath)
			throw new DScabiException("Storage Dir Path is null", "DPN.DEN.1");
		if (null == arrayFolder)
			throw new DScabiException("Array Folder is null", "DPN.DEN.2");
		if (storageDirPath.length() == 0)
			throw new DScabiException("Storage Dir Path is empty string", "DPN.DEN.3");
		if (arrayFolder.length() == 0)
			throw new DScabiException("Array Folder is empty string", "DPN.DEN.4");
		if (null == deletedBy)
			throw new DScabiException("deletedBy is null", "DPN.DEN.5");
		if (deletedBy.length() == 0)
			throw new DScabiException("deletedBy is empty string", "DPN.DEN.5");
		
		try {
			createPartitionIdDeletedFile(storageDirPath, arrayFolder, localDirPath, storageHandler, "Deleted By : " + deletedBy);
		} catch (Exception e) {
			throw new IOException(e);
		}
		
		logs.debug("deletePartition(...) Proceeding with isPartitionIdFileExists() check for partition : {}", arrayFolder);			   		
		boolean check = DataPartition.isPartitionIdFileExists(storageDirPath, arrayFolder, storageHandler);

		if (check) {
			logs.debug("deletePartition(...) Information only - Partition Id file exists for partition : {}, storageDirPath : {}", arrayFolder, storageDirPath);

			String detailsStrPid = DataPartition.getPartitionIdFileData(storageDirPath, arrayFolder, localDirPath, storageHandler);
			if (null == detailsStrPid) {
				logs.debug("deletePartition(...) Information only - detailsStrPid is null for DataPartition for partition : " + arrayFolder + " storageDirPath : " + storageDirPath);
				return 0;
			} else {
				if (detailsStrPid.length() == 0) {
					logs.debug("deletePartition(...) Information only - Details in partition id file : " + arrayFolder + ".txt is empty" + " storageDirPath : " + storageDirPath);					
					return 0;
				} else {	
					/* cw
					String s[] = detailsStrPid.split(";");
					// This number 5 should be consistent with how many ";" we write in operationsSuccess() method
					if (s.length < 5) {					
						logs.debug("deletePartition(...) Information only - Details in partition id file : " + arrayFolder + ".txt is not in correct format. Details is : " + detailsStr);					
						return 0;		
					} else if (false == s[1].equalsIgnoreCase("SUCCESS")) {
						logs.debug("deletePartition(...) Information only - Status in partition id file : " + arrayFolder + ".txt is not SUCCESS. Status is : " + s[1]);
						return 0;
					}
					else {
						retryNumber = Long.parseLong(s[0]);
						proceed = true;
					}
					*/

					try {
						djsonPid = new DMJson(detailsStrPid);
						// to use same value from the partitionid.txt file
						strAppId = djsonPid.getString("AppId");
						strDataId = djsonPid.getString("DataId");
						strSplitUnit = djsonPid.getString("SplitUnit");
						strPartitionUserRef = djsonPid.getString("PartitionUserRef");
						strArrayFolder = djsonPid.getString("ArrayFolder");
						strRetryNumber = djsonPid.getString("RetryNumber");
						strParallelNumber = djsonPid.getString("ParallelNumber");
						strTotalNumOfBigArrays = djsonPid.getString("TotalNumOfBigArrays");
						strMaxRetry = djsonPid.getString("MaxRetry");
						strMaxParallel = djsonPid.getString("MaxParallel");
						strStatus = djsonPid.getString("Status");
						strCreatedBy = djsonPid.getString("CreatedBy");
						
						retryNumber = Long.parseLong(strRetryNumber);
						parallelNumber = Long.parseLong(strParallelNumber);
						totalNumOfBigArrays = Long.parseLong(strTotalNumOfBigArrays);
					} catch (Exception e) {
						logs.debug("deletePartition(...) Information only - Details in partition id file : " + arrayFolder + ".txt is not in correct format. Details is : " + detailsStrPid + " storageDirPath : " + storageDirPath);					
						return 0;		
					}
					// cw if (false == strStatus.equalsIgnoreCase("SUCCESS")) {
					// cw 	logs.debug("deletePartition(...) Information only - Status in partition id file : " + arrayFolder + ".txt is not SUCCESS. Status is : " + strStatus + " storageDirPath : " + storageDirPath);
					// cw	return 0;
					// cw } else {
					proceed = true;
					// cw }
				}
			}
		} else {
			logs.debug("deletePartition(...) Information only - Partition Id file does not exist for partition : {}, storageDirPath : {}", arrayFolder, storageDirPath);
			return 0;
		}	
		
		if (proceed) {
	
			/* cw
			if (dirPath.charAt(dirPath.length() - 1) == File.separatorChar)
				dirPath = dirPath + arrayFolder;
			else
				dirPath = dirPath + File.separator + arrayFolder;
			logs.debug("deletePartition(...) dirPath : {}", dirPath);
			*/
			
			try {
				// cw String deleteDetailsStr = retryNumber + ";DELETED;actualArrayFolder=" + actualArrayFolder +";<partitionid>.txt=" + arrayFolder + ".txt;<partitionid>_RNCC_<rn>.txt=" + arrayFolder + "_RNCC_" + retryNumber + ".txt";
				Instant inst = Instant.now();
				long nano = System.nanoTime();
				DMJson djsonDel = new DMJson();
				// to use same value from the partitionid.txt file
				djsonDel.add("AppId", strAppId);
				djsonDel.add("DataId", strDataId);
				djsonDel.add("SplitUnit", strSplitUnit);
				djsonDel.add("PartitionUserRef", strPartitionUserRef);
				djsonDel.add("ArrayFolder", strArrayFolder);
				djsonDel.add("RetryNumber", strRetryNumber);
				djsonDel.add("ParallelNumber", strParallelNumber);
				djsonDel.add("TotalNumOfBigArrays", strTotalNumOfBigArrays);
				djsonDel.add("MaxRetry", strMaxRetry);
				djsonDel.add("MaxParallel", strMaxParallel);
				djsonDel.add("Timestamp", inst.toString());
				djsonDel.add("NanoTimestamp", "" + nano);
				djsonDel.add("Status", "DELETED");
				djsonDel.add("CreatedBy", strCreatedBy);
				djsonDel.add("Remarks", "Deleted By : " + deletedBy);
				
				String deleteDetailsStr = djsonDel.toString();
				createPartitionIdDeletedFile(storageDirPath, arrayFolder, localDirPath, storageHandler, deleteDetailsStr);
				
				deletePartitionIdRPCCFile(storageDirPath, arrayFolder, storageHandler, retryNumber, parallelNumber);
				deletePartitionIdFile(storageDirPath, arrayFolder, storageHandler);
			
				for (long i = 1; i <= totalNumOfBigArrays; i++) {
					String actualArrayFolder = arrayFolder + "_R" + retryNumber + "_P" + parallelNumber + "_B" + i;
					String dirPath = storageDirPath;
		
					if (dirPath.endsWith(File.separator))
						dirPath = dirPath + actualArrayFolder;
					else
						dirPath = dirPath + File.separator + actualArrayFolder;
					logs.debug("deletePartition(...) dirPath : {} for BigArray : {}", dirPath, i);
				
					storageHandler.deleteArrayDirIfExists(dirPath);		
				}
				
				deletePartitionIdPRECNBAFile(storageDirPath, arrayFolder, storageHandler, retryNumber, parallelNumber);
				deletePartitionIdPOSTCNBAFile(storageDirPath, arrayFolder, storageHandler, retryNumber, parallelNumber);
				
			} catch (Exception e) {
				throw new IOException(e);
			}
		} // End if (proceed)
		
		// System.out.println("deletePartition(...) Deleting data partition : " + arrayFolder + " done");
		logs.debug("deletePartition(...) Deleting data partition : {} storageDirPath : {} done", arrayFolder, storageDirPath);
		
		return 0;
	}

	public int deletePartition() throws Exception {
		close();
		
		// System.out.println("deletePartition() Deleting data partition : " + m_arrayFolder + "...");
		log.debug("deletePartition() Deleting data partition : {}...", m_arrayFolder);
		
		/* cw
		if (dirPath.charAt(dirPath.length() - 1) == File.separatorChar)
			dirPath = dirPath + m_arrayFolder;
		else
			dirPath = dirPath + File.separator + m_arrayFolder;
		*/
		
		try {
			
			// cw String deleteDetailsStr = m_retryNumber + ";DELETED;actualArrayFolder=" + m_actualArrayFolder +";<partitionid>.txt=" + m_arrayFolder + ".txt;<partitionid>_RNCC_<rn>.txt=" + m_arrayFolder + "_RNCC_" + m_retryNumber + ".txt";
			
			Instant inst = Instant.now();
			long nano = System.nanoTime();
			DMJson djsonDel = new DMJson();
			djsonDel.add("AppId", m_context.getAppId());
			djsonDel.add("DataId", m_dataId);
			djsonDel.add("SplitUnit", "" + m_context.getDU());
			djsonDel.add("PartitionUserRef", m_partitionUserRef);
			djsonDel.add("ArrayFolder", m_arrayFolder);
			djsonDel.add("RetryNumber", "" + m_retryNumber);
			djsonDel.add("ParallelNumber", "" + m_parallelNumber);
			djsonDel.add("TotalNumOfBigArrays", "" + m_totalNumOfBigArrays);
			djsonDel.add("MaxRetry", "" + m_context.getMaxRetry());
			djsonDel.add("MaxParallel", "" + m_context.getMaxParallel());
			djsonDel.add("Timestamp", inst.toString());
			djsonDel.add("NanoTimestamp", "" + nano);
			djsonDel.add("Status", "DELETED");
			djsonDel.add("CreatedBy", m_createdBy);
			djsonDel.add("Remarks", "Deleted By : non-static DataPartition.deletePartition() with App Id m_appId : " + m_context.getAppId());
			
			String deleteDetailsStr = djsonDel.toString();
			
			createPartitionIdDeletedFile(m_storageDirPath, m_arrayFolder, m_localDirPath, m_storageHandler, deleteDetailsStr);

			deletePartitionIdRPCCFile();
			deletePartitionIdFile();	
			
			for (long i = 1; i <= m_totalNumOfBigArrays; i++) {
				String dirPath = m_storageDirPath;
				if (dirPath.endsWith(File.separator))
					dirPath = dirPath + m_arrayFolder + "_R" + m_retryNumber + "_P" + m_parallelNumber + "_B" + i;
				else
					dirPath = dirPath + File.separator + m_arrayFolder + "_R" + m_retryNumber + "_P" + m_parallelNumber + "_B" + i;;
				log.debug("deletePartition() dirPath : {} for BigArray : {}", dirPath, i);
				m_storageHandler.deleteArrayDirIfExists(dirPath);
			}
			
			deletePartitionIdPRECNBAFile(m_storageDirPath, m_arrayFolder, m_storageHandler, m_retryNumber, m_parallelNumber);
			deletePartitionIdPOSTCNBAFile(m_storageDirPath, m_arrayFolder, m_storageHandler, m_retryNumber, m_parallelNumber);
			
			if (m_isNew)
				m_isNew = false;
		} catch (Exception e) {
			throw new IOException(e);
		}
		
		// System.out.println("deletePartition() Deleting data partition : " + m_arrayFolder + " done");
		log.debug("deletePartition() Deleting data partition : {} storageDirPath : {} done", m_arrayFolder, m_storageDirPath);
		
		return 0;
	}	
	
	public int deletePartitionWithoutDeletedFile() throws Exception {
		close();
		
		// System.out.println("deletePartitionWithoutDeletedFile() Deleting data partition : " + m_arrayFolder + "...");
		log.debug("deletePartitionWithoutDeletedFile() Deleting data partition : {}...", m_arrayFolder);
	
		try {
			deletePartitionIdRPCCFile();
			deletePartitionIdFile();	
			
			for (long i = 1; i <= m_totalNumOfBigArrays; i++) {
				String dirPath = m_storageDirPath;
				if (dirPath.endsWith(File.separator))
					dirPath = dirPath + m_arrayFolder + "_R" + m_retryNumber + "_P" + m_parallelNumber + "_B" + i;
				else
					dirPath = dirPath + File.separator + m_arrayFolder + "_R" + m_retryNumber + "_P" + m_parallelNumber + "_B" + i;;
				log.debug("deletePartitionWithoutDeletedFile() dirPath : {} for BigArray : {}", dirPath, i);
				m_storageHandler.deleteArrayDirIfExists(dirPath);
			}
			
			deletePartitionIdPRECNBAFile(m_storageDirPath, m_arrayFolder, m_storageHandler, m_retryNumber, m_parallelNumber);
			deletePartitionIdPOSTCNBAFile(m_storageDirPath, m_arrayFolder, m_storageHandler, m_retryNumber, m_parallelNumber);
			
			if (m_isNew)
				m_isNew = false;
		} catch (Exception e) {
			throw new IOException(e);
		}
		
		// System.out.println("deletePartitionWithoutDeletedFile() Deleting data partition : " + m_arrayFolder + " done");
		log.debug("deletePartitionWithoutDeletedFile() Deleting data partition : {} storageDirPath : {} done", m_arrayFolder, m_storageDirPath);
		
		return 0;
	}	
	
	public static DataPartition createDataPartition(DataContext c, String dataId, String partitionUserRef, String storageDirPath, String arrayFolder, int pageSize, String localDirPath, IStorageHandler storageHandler, String createdBy) throws Exception {	
	
		logs.debug("createDataPartition() partition : {}", arrayFolder);
		
		logs.debug("createDataPartition() Proceeding with isPartitionExists() check for partition : {}", arrayFolder);			   		
		boolean check = DataPartition.isPartitionIdFileExists(storageDirPath, arrayFolder, storageHandler);
			
		DataPartition dp = null;
		if (false == check) {
			logs.debug("createDataPartition() Information only - Partition doesn't exist for partition : {}, storageDirPath : {}", arrayFolder, storageDirPath);
			dp = new DataPartition(c, dataId, partitionUserRef, storageDirPath, arrayFolder, pageSize, localDirPath, storageHandler, createdBy);
		} else {
			throw new DScabiException("DataPartition for partition : " + arrayFolder + " already found in Storage system", "DPN.CDP.1");			   	
		}		
		
		return dp;
	}
	
	public static DataPartition allowCreateDataPartition(DataContext c, String dataId, String partitionUserRef, String storageDirPath, String arrayFolder, int pageSize, String localDirPath, IStorageHandler storageHandler, String createdBy) throws Exception {	
		
		logs.debug("allowCreateDataPartition() partition : {}", arrayFolder);
		
		logs.debug("allowCreateDataPartition() Proceeding with isPartitionIdDeletedFileExists() check for partition : {}", arrayFolder);			   		
		boolean checkDeleted = isPartitionIdDeletedFileExists(storageDirPath, arrayFolder, storageHandler);
		if (checkDeleted) {
			throw new DScabiException("DataPartition for partition : " + arrayFolder + " is marked as deleted in Storage system", "DPN.ACD.1");
		}
		
		logs.debug("allowCreateDataPartition() Proceeding with isPartitionIdFileExists() check for partition : {}", arrayFolder);			   		
		boolean check3 = DataPartition.isPartitionIdFileExists(storageDirPath, arrayFolder, storageHandler);
		boolean proceed = false;	
		long retryNumber = -1;
		long parallelNumber = -1;
		
		if (check3) {
			logs.debug("allowCreateDataPartition() Information only - Partition Id file exists for partition : {}, storageDirPath : {}", arrayFolder, storageDirPath);

			String detailsStrPid2 = DataPartition.getPartitionIdFileData(storageDirPath, arrayFolder, localDirPath, storageHandler);
			if (null == detailsStrPid2) {
				logs.debug("allowCreateDataPartition() detailsStr2 is null for DataPartition for partition : " + arrayFolder + " Proceeding to create data partition");
				proceed = true;
			} else {
				if (detailsStrPid2.length() == 0) {
					logs.debug("allowCreateDataPartition() Details in partition id file : " + arrayFolder + ".txt is empty" + " Proceeding to create data partition");					
					proceed = true;
				} else {	
					// cw String s[] = detailsStr2.split(";");
					// cw // This number 5 should be consistent with how many ";" we write here in the end in operationsSuccess() method
					// cw if (s.length < 5) {					
					// cw	logs.debug("allowCreateDataPartition() Details in partition id file : " + arrayFolder + ".txt is not in correct format. Details is : " + detailsStr2 + " Proceeding to create data partition");					
					// cw	proceed = true;		
					// cw } else {
					// cw	retryNumber = Long.parseLong(s[0]);
						
					try {	
						// This try-catch is only for getting values from json
						// Will get exception if detailsStr is not in proper
						// json format
						DMJson djsonPid2 = new DMJson(detailsStrPid2);	
						retryNumber = djsonPid2.getLongOf("RetryNumber");
						parallelNumber = djsonPid2.getLongOf("ParallelNumber");
					} catch (Exception e) {
						logs.debug("allowCreateDataPartition() Details in partition id file : " + arrayFolder + ".txt is not in correct format. Details is : " + detailsStrPid2 + " Proceeding to create partition id file");					
						proceed = true;
					}
					if (false == proceed) {	
						if (parallelNumber == c.getParallelNumber()) {
							if (retryNumber == c.getRetryNumber()) {
								throw new DScabiException("Partition id file is already having retry number same as this DU's retry number. c.getRetryNumber() : " + c.getRetryNumber() + " retryNumber : " + retryNumber + " c.getParallelNumber() : " + c.getParallelNumber() + " parallelNumber : " + parallelNumber + " arrayFolder : " + arrayFolder, "DPN.ACD.1");
							} else if (retryNumber < c.getRetryNumber()) {
								logs.debug("allowCreateDataPartition() Information only - case retryNumber < c.getRetryNumber() Proceeding to create data partition");
								proceed = true;
							} else {
								// retryNumber > c.getRetryNumber()
								throw new DScabiException("Partition id file is already having retry number greater than this DU's retry number. c.getRetryNumber() : " + c.getRetryNumber() + " retryNumber : " + retryNumber + " c.getParallelNumber() : " + c.getParallelNumber() + " parallelNumber : " + parallelNumber + " arrayFolder : " + arrayFolder, "DPN.OSS.1");
								/* cw
								// This commented code below is old logic where partitionid_RNCC_<r>.txt file was created after creating partitionid.txt file
								logs.debug("allowCreateDataPartition() Information only - case retryNumber > c.getRetryNumber() Proceeding to check if Partition Id RNCC file exists for partition : {}, storageDirPath : {}, retryNumber : {}", arrayFolder, storageDirPath, retryNumber);
								boolean check4 = DataPartition.isPartitionIdRPCCFileExists(storageDirPath, arrayFolder, storageHandler, retryNumber, parallelNumber);
								if (check4) {
									logs.debug("allowCreateDataPartition() Information only - Partition Id RNCC file exists for partition : {}, storageDirPath : {}, retryNumber : {}, parallelNumber : {}", arrayFolder, storageDirPath, retryNumber, parallelNumber);
									throw new DScabiException("Partition id file is already having retry number greater than this DU's retry number. c.getRetryNumber() : " + c.getRetryNumber() + " retryNumber : " + retryNumber + " c.getParallelNumber() : " + c.getParallelNumber() + " parallelNumber : " + parallelNumber + " arrayFolder : " + arrayFolder, "DPN.ACD.1");
								} else {
									logs.debug("allowCreateDataPartition() Information only - Partition Id RNCC file does not exist for partition : {}, storageDirPath : {}, retryNumber : {} , parallelNumber : {} Proceeding to create data partition", arrayFolder, storageDirPath, retryNumber, parallelNumber);
									proceed = true;
								}	
								*/
							}
						} else {
							proceed = true;
						}
					}
					// cw }
				}
			}
		} else {
			logs.debug("allowCreateDataPartition() Information only - Partition Id file does not exist for partition : {}, storageDirPath : {} Proceeding to create data partition", arrayFolder, storageDirPath);
			proceed = true;
		}	

		DataPartition dp = null;
		if (proceed) {
			dp = new DataPartition(c, dataId, partitionUserRef, storageDirPath, arrayFolder, pageSize, localDirPath, storageHandler, createdBy);
		}
		
		return dp;
		
		/* cw
		logs.debug("allowCreateDataPartition() Proceeding with isPartitionExists() check for partition : {}", arrayFolder);			   		
		boolean check = DataPartition.isPartitionIdFileExists(storageDirPath, arrayFolder, storageHandler);
			
		DataPartition dp = null;
		if (false == check) {
			logs.debug("allowCreateDataPartition() Information only - Partition doesn't exist for partition : {}, storageDirPath : {}", arrayFolder, storageDirPath);
			dp = new DataPartition(c, dataId, partitionUserRef, storageDirPath, arrayFolder, pageSize, localDirPath, storageHandler, true);
		} else {
			dp = new DataPartition(c, dataId, partitionUserRef, storageDirPath, arrayFolder, pageSize, localDirPath, storageHandler, true);
			// TODO delete local partition id file if exists
			// copyifexists partition id file to local
			// read retry number from local partition id file
			// delete if exists local partition id file
			// if retry number < c.getRetryNumber() then
			// deleteifexists partition id file
			// dp = new DataPartition(...);
			// else 
			// throw new DScabiException("DataPartition for partition : " + arrayFolder + " already found in Storage system with higher retry number " + retrynumber, "DPN.CDP.1");			   	
		}		
		
		return dp;
		*/
	}	
	
	/* cw
	public static DataPartition readDataPartition(DataContext c, String dataId, String partitionUserRef, String storageDirPath, String arrayFolder, int pageSize, String localDirPath, IStorageHandler storageHandler) throws Exception {	
		
		logs.debug("readDataPartition() partition : {}", arrayFolder);
		
		logs.debug("readDataPartition() Proceeding with isPartitionExists() check for partition : {}", arrayFolder);			   		
		boolean check = DataPartition.isPartitionExists(storageDirPath, arrayFolder, storageHandler);
			
		DataPartition dp = null;
		if (check) {
			logs.debug("readDataPartition() Information only - Partition exists for partition : {}, storageDirPath : {}", arrayFolder, storageDirPath);
			dp = new DataPartition(c, dataId, partitionUserRef, storageDirPath, arrayFolder, pageSize, localDirPath, storageHandler, false);
		} else {
			throw new DScabiException("DataPartition for partition : " + arrayFolder + " is not found in Storage system", "DPN.RDP.1");
		}		
		
		return dp;
	}	
	*/	
	
	public static DataPartition readDataPartition(DataContext c, String dataId, String partitionUserRef, String storageDirPath, String arrayFolder, int pageSize, String localDirPath, IStorageHandler storageHandler, String readBy) throws Exception {	
		
		boolean toRepair = false;
		long retryNumber = -1;
		long maxRetry = -1;
		long parallelNumber = -1;
		long maxParallel = -1;
		long totalNumOfBigArrays = -1;
		
		logs.debug("readDataPartition() partition : {}", arrayFolder);
		maxRetry = c.getMaxRetry();
		maxParallel = c.getMaxParallel();
		
		logs.debug("readDataPartition() Proceeding with isPartitionIdDeletedFileExists() check for partition : {}", arrayFolder);			   		
		boolean checkDeleted = isPartitionIdDeletedFileExists(storageDirPath, arrayFolder, storageHandler);
		if (checkDeleted) {
			throw new DScabiException("DataPartition for partition : " + arrayFolder + " is marked as deleted in Storage system", "DPN.RDP.1");
		}
		
		logs.debug("readDataPartition() Proceeding with isPartitionIdFileExists() check for partition : {}", arrayFolder);			   		
		boolean check = DataPartition.isPartitionIdFileExists(storageDirPath, arrayFolder, storageHandler);
			
		DataPartition dp = null;
		if (check) {
			logs.debug("readDataPartition() Information only - Partition Id file exists for partition : {}, storageDirPath : {}", arrayFolder, storageDirPath);

			String detailsStrPid = DataPartition.getPartitionIdFileData(storageDirPath, arrayFolder, localDirPath, storageHandler);
			if (null == detailsStrPid) {
				logs.debug("readDataPartition() detailsStrPid is null for DataPartition for partition : " + arrayFolder + " Proceeding to try to repair");
				toRepair = true;
			} else {
				if (detailsStrPid.length() == 0) {
					logs.debug("readDataPartition() Details in partition id file : " + arrayFolder + ".txt is empty" + " Proceeding to try to repair");					
					toRepair = true;
				} else {	
					// cw String s[] = detailsStrPid.split(";");
					// This number 5 should be consistent with how many ";" we write in operationsSuccess() method
					// cw if (s.length < 5) {					
					// cw	logs.debug("readDataPartition() Details in partition id file : " + arrayFolder + ".txt is not in correct format. Details is : " + detailsStr + " Proceeding to try to repair");					
					// cw	toRepair = true;		
					// cw } else if (false == s[1].equalsIgnoreCase("SUCCESS")) {
					// cw	throw new DScabiException("Status in partition id file : " + arrayFolder + ".txt is not SUCCESS. Status is : " + s[1], "DPN.DPN.2");
					// cw } else {
					// cw 	retryNumber = Long.parseLong(s[0]);
					String status = null;
					String createdBy = null;
					try {
						// This try-catch is only for getting values from json
						// Will get exception if detailsStr is not in proper
						// json format
						DMJson djsonPid = new DMJson(detailsStrPid);
						retryNumber = djsonPid.getLongOf("RetryNumber");
						parallelNumber = djsonPid.getLongOf("ParallelNumber");
						totalNumOfBigArrays = djsonPid.getLongOf("TotalNumOfBigArrays");
						status = djsonPid.getString("Status");
						createdBy = djsonPid.getString("CreatedBy");
					} catch (Exception e) {
						logs.debug("readDataPartition() Details in partition id file : " + arrayFolder + ".txt is not in correct format. Details is : " + detailsStrPid + " Proceeding to try to repair");					
						toRepair = true;
					}
					if (false == toRepair) {
						if (false == status.equalsIgnoreCase("SUCCESS")) {
							throw new DScabiException("Status in partition id file : " + arrayFolder + ".txt is not SUCCESS. Status is : " + status, "DPN.DPN.2");
						}
						boolean check2 = DataPartition.isPartitionIdRPCCFileExists(storageDirPath, arrayFolder, storageHandler, retryNumber, parallelNumber);
						if (check2) {
							logs.debug("readDataPartition() Information only - Partition Id RPCC file exists for partition : {}, storageDirPath : {}, retryNumber : {}, parallelNumber : {}", arrayFolder, storageDirPath, retryNumber, parallelNumber);
							dp = new DataPartition(c, dataId, partitionUserRef, storageDirPath, arrayFolder, pageSize, localDirPath, storageHandler, retryNumber, parallelNumber, totalNumOfBigArrays, createdBy);
						} else {
							logs.debug("readDataPartition() Information only - Partition Id RPCC file does not exist for partition : {}, storageDirPath : {}, retryNumber : {}, parallelNumber : {} Proceeding to try to repair", arrayFolder, storageDirPath, retryNumber, parallelNumber);
							toRepair = true;
						}	
					}
					// cw } // for else part
				}
			}
		} else {
			logs.debug("readDataPartition() Information only - Partition Id file does not exist for partition : {}, storageDirPath : {} Proceeding to try to repair", arrayFolder, storageDirPath);
			toRepair = true;
		}		

		boolean found = false;
		if (toRepair) {
			logs.debug("readDataPartition() Proceeding with repair of partition : {}, storageDirPath : {}", arrayFolder, storageDirPath);
			long i = 0;
			/* cw
			if (retryNumber != -1)
				i = retryNumber + 1;
			*/
			if (-1 == maxRetry)
				throw new DScabiException("Max Retry is not set in data context", "DPN.RDP.1");
			logs.debug("readDataPartition() maxRetry : {}", maxRetry);
			if (-1 == maxParallel)
				throw new DScabiException("Max Parallel is not set in data context", "DPN.RDP.1");
			logs.debug("readDataPartition() maxParallel : {}", maxParallel);

			for (long p = 0; p <= maxParallel; p++) {
				// i <= maxRetry this is correct as i=0 is first run of crun and is not a retry
				for ( ; i <= maxRetry; i++) {
					// check RPCC files exists for _RNCC_i_p.txt
					boolean check3 = DataPartition.isPartitionIdRPCCFileExists(storageDirPath, arrayFolder, storageHandler, i, p);
					if (check3) {
						logs.debug("readDataPartition() Information only - Partition Id RPCC file exists for partition : {}, storageDirPath : {}, retryNumber : {}, parallelNumber : {}", arrayFolder, storageDirPath, i, p);
						String detailsStrRPCC1 = DataPartition.getPartitionIdRPCCFileData(storageDirPath, arrayFolder, localDirPath, storageHandler, i, p);
						if (null == detailsStrRPCC1) {
							logs.debug("operationsSuccessWithAppStatusCheck() Information only - detailsStrRPCC1 is null for partition : {}, storageDirPath : {}, retryNumber : {} parallelNumber : {} Proceeding to check next partition id RPCC file", arrayFolder, storageDirPath, i, p);
							continue;
						} else {
							if (detailsStrRPCC1.length() == 0) {
								logs.debug("operationsSuccessWithAppStatusCheck() Information only - Details in partition id RPCC file.txt is empty for partition : {}, storageDirPath : {}, retryNumber : {} parallelNumber : {} Proceeding to check next partition id RPCC file", arrayFolder, storageDirPath, i, p);
								continue;
							} else {	
								String strAppId, strDataId, strSplitUnit, strPartitionUserRef, strArrayFolder;
								String strRetryNumber, strParallelNumber, strTotalNumOfBigArrays;
								String strMaxRetry, strMaxParallel;
								String strCreatedBy, strStatus = null;
								try {
									// readDataPartition() method is also called from
									// Data.getDataPartition() method
									// So put only the original values from _RPCC_i_p.txt file into partition id.txt file
									DMJson djsonRPCC1 = new DMJson(detailsStrRPCC1);
									// to use same value from the RPCC file
									strAppId = djsonRPCC1.getString("AppId");
									strDataId = djsonRPCC1.getString("DataId");
									strSplitUnit = djsonRPCC1.getString("SplitUnit");
									strPartitionUserRef = djsonRPCC1.getString("PartitionUserRef");
									strArrayFolder = djsonRPCC1.getString("ArrayFolder");
									strRetryNumber = djsonRPCC1.getString("RetryNumber");
									strParallelNumber = djsonRPCC1.getString("ParallelNumber");
									strTotalNumOfBigArrays = djsonRPCC1.getString("TotalNumOfBigArrays");
									strMaxRetry = djsonRPCC1.getString("MaxRetry");
									strMaxParallel = djsonRPCC1.getString("MaxParallel");
									strStatus = djsonRPCC1.getString("Status");
									strCreatedBy = djsonRPCC1.getString("CreatedBy");
									
									retryNumber = Long.parseLong(strRetryNumber);
									parallelNumber = Long.parseLong(strParallelNumber);
									totalNumOfBigArrays = Long.parseLong(strTotalNumOfBigArrays);
								} catch (Exception e) {
									logs.debug("readDataPartition() Details in partition id RPCC file is not in correct format. Details is : {}, partition : {}, storageDirPath : {}, retryNumber : {} parallelNumber : {} Proceeding to check next partition id RPCC file", detailsStrRPCC1, arrayFolder, storageDirPath, i, p);					
									continue;
								}
								if (false == strStatus.equalsIgnoreCase("SUCCESS")) {
									logs.debug("readDataPartition() Status in partition id RPCC file is not SUCCESS. Status is : {}, partition : {}, storageDirPath : {}, retryNumber : {} parallelNumber : {} Proceeding to check next partition id RPCC file", strStatus, arrayFolder, storageDirPath, i, p);	
									continue;
								}
								// if _RPCC_i_p.txt file exists, create partition id file with retry number = i, parallel number = p, create dp, found = true and break for loop 
								Instant inst = Instant.now();
								long nano = System.nanoTime();
								// to put same value from the RPCC file
								DMJson djsonPid2 = new DMJson();
								djsonPid2.add("AppId", strAppId);
								djsonPid2.add("DataId", strDataId);
								djsonPid2.add("SplitUnit", strSplitUnit);
								djsonPid2.add("PartitionUserRef", strPartitionUserRef);
								djsonPid2.add("ArrayFolder", strArrayFolder);
								djsonPid2.add("RetryNumber", strRetryNumber);
								djsonPid2.add("ParallelNumber", strParallelNumber);
								djsonPid2.add("TotalNumOfBigArrays", strTotalNumOfBigArrays);
								djsonPid2.add("MaxRetry", strMaxRetry);
								djsonPid2.add("MaxParallel", strMaxParallel);
								djsonPid2.add("Timestamp", inst.toString());
								djsonPid2.add("NanoTimestamp", "" + nano);
								djsonPid2.add("Status", "SUCCESS");
								djsonPid2.add("CreatedBy", strCreatedBy);
								djsonPid2.add("Remarks", "Repaired By : " + readBy);
								
								// cw String detailsStrPid2 = i + ";SUCCESS;" + inst.toString() + ";" + nano + ";Repaired by DataPartition.readDataPartition()";
								String detailsStrPid2 = djsonPid2.toString();
								createPartitionIdFile(storageDirPath, arrayFolder, localDirPath, storageHandler, detailsStrPid2);
								logs.debug("readDataPartition() Repair completed for partition : " + arrayFolder + " storageDirPath : " + storageDirPath + " Repaired By : " + readBy);
								
								dp = new DataPartition(c, dataId, partitionUserRef, storageDirPath, arrayFolder, pageSize, localDirPath, storageHandler, retryNumber, parallelNumber, totalNumOfBigArrays, strCreatedBy);
								found = true;
								break;
							}
						}
					}
				}
				
				if (found)
					break;
				
			}
			
			if (false == found)
				throw new DScabiException("DataPartition for partition : " + arrayFolder + " storageDirPath : " + storageDirPath + " is not found in Storage system", "DPN.RDP.1");
		}
		
		return dp;
	}		

	
	/* pw
	// This is given default access
	DataPartition(DataContext c, String dataId, String partitionUserRef, String storageDirPath, String arrayFolder, int pageSize, String localDirPath, IStorageHandler storageHandler) throws Exception {

		if (null == c)
			throw new DScabiException("Data Context is null", "DPN.DPN.1");
		if (null == dataId)
			throw new DScabiException("Data Id is null", "DPN.DPN.2");
		if (null == partitionUserRef)
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
			if (localDirPath.endsWith(File.separator))
				deleteLocalPartitionIfExists(localDirPath + arrayFolder);
			else
				deleteLocalPartitionIfExists(localDirPath + File.separator + arrayFolder);
				
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
			m_partitionUserRef = partitionUserRef;
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
	*/
	
	/* cw 18-Feb-2017
	// This is given default access
	DataPartition(DataContext c, String dataId, String partitionUserRef, String storageDirPath, String arrayFolder, int pageSize, String localDirPath, IStorageHandler storageHandler, boolean isNew) throws Exception {
		// This constructor is used for creating DataPartition
		// pass isNew as true
		if (null == c)
			throw new DScabiException("Data Context is null", "DPN.DPN.1");
		if (null == dataId)
			throw new DScabiException("Data Id is null", "DPN.DPN.2");
		if (null == partitionUserRef)
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
			if (isNew) {
				m_retryNumber = c.getRetryNumber();
				m_parallelNumber = c.getParallelNumber();
				m_actualArrayFolder = arrayFolder + "_R" + m_retryNumber + "_P" + m_parallelNumber + "_B" + (m_lastBigArray + 1);
				
				if (localDirPath.endsWith(File.separator))
					deleteLocalPartitionIfExists(localDirPath + m_actualArrayFolder);
				else
					deleteLocalPartitionIfExists(localDirPath + File.separator + m_actualArrayFolder);
				
				// Delete in storageDirPath is not needed as of 18-Feb-2017
				// cw if (storageDirPath.endsWith(File.separator))
				// cw 	storageHandler.deleteDirIfExists(storageDirPath + m_actualArrayFolder);
				// cw else
				// cw	storageHandler.deleteDirIfExists(storageDirPath + File.separator + m_actualArrayFolder);
				// OR
				// cw if (storageDirPath.endsWith(File.separator))
				// cw	storageHandler.deleteIfExists(storageDirPath + m_actualArrayFolder + File.separator + "meta_data" + File.separator + "page-0.dat");
				// cw else
				// cw 	storageHandler.deleteIfExists(storageDirPath + File.separator + m_actualArrayFolder + File.separator + "meta_data" + File.separator + "page-0.dat");
				
			} else {
				throw new DScabiException("This constructor should not be used for reading a data partition", "DPN.DPN.1");
			}
			
			m_isNew = isNew;
			m_context = c;
			m_storageDirPath = storageDirPath;
			m_arrayFolder = arrayFolder;
			m_localDirPath = localDirPath;
			m_storageHandler = storageHandler;
			m_dataId = dataId;
			m_partitionUserRef = partitionUserRef;
			m_pageSize = pageSize;
			
			if (isNew) {
				Instant inst = Instant.now();
				long nano = System.nanoTime();
				DMJson djsonPRECNBA = new DMJson();
				djsonPRECNBA.add("AppId", m_context.getAppId());
				djsonPRECNBA.add("DataId", m_dataId);
				djsonPRECNBA.add("SplitUnit", "" + m_context.getDU());
				djsonPRECNBA.add("PartitionUserRef", m_partitionUserRef);
				djsonPRECNBA.add("ArrayFolder", m_arrayFolder);
				djsonPRECNBA.add("RetryNumber", "" + m_retryNumber);
				djsonPRECNBA.add("ParallelNumber", "" + m_parallelNumber);
				djsonPRECNBA.add("PreCreateLastBigArray", "" + m_lastBigArray);				
				djsonPRECNBA.add("MaxRetry", "" + m_context.getMaxRetry());
				djsonPRECNBA.add("MaxParallel", "" + m_context.getMaxParallel());
				djsonPRECNBA.add("Timestamp", inst.toString());
				djsonPRECNBA.add("NanoTimestamp", "" + nano);

				String detailsStrPRECNBA = djsonPRECNBA.toString();
				createPartitionIdPRECNBAFile(m_storageDirPath, m_arrayFolder, m_localDirPath, m_storageHandler, m_retryNumber, m_parallelNumber, detailsStrPRECNBA);
			}
			
			m_bigArray = new BigArrayImpl(storageDirPath, m_actualArrayFolder, pageSize, localDirPath, m_storageHandler);			
			if (null == m_bigArray) {
				throw new DScabiException("big array is null", "DPN.DPN.10");
			} 

			m_currentBigArray++;
			m_bigArrayMap.put("" + m_currentBigArray, m_bigArray);

			if (isNew) {
				m_totalNumOfBigArrays++;				
				m_lastBigArray++;
				Instant inst = Instant.now();
				long nano = System.nanoTime();
				DMJson djsonPOSTCNBA = new DMJson();
				djsonPOSTCNBA.add("AppId", m_context.getAppId());
				djsonPOSTCNBA.add("DataId", m_dataId);
				djsonPOSTCNBA.add("SplitUnit", "" + m_context.getDU());
				djsonPOSTCNBA.add("PartitionUserRef", m_partitionUserRef);
				djsonPOSTCNBA.add("ArrayFolder", m_arrayFolder);
				djsonPOSTCNBA.add("RetryNumber", "" + m_retryNumber);
				djsonPOSTCNBA.add("ParallelNumber", "" + m_parallelNumber);
				djsonPOSTCNBA.add("PostCreateLastBigArray", "" + m_lastBigArray);
				djsonPOSTCNBA.add("MaxRetry", "" + m_context.getMaxRetry());
				djsonPOSTCNBA.add("MaxParallel", "" + m_context.getMaxParallel());
				djsonPOSTCNBA.add("Timestamp", inst.toString());
				djsonPOSTCNBA.add("NanoTimestamp", "" + nano);

				String detailsStrPOSTCNBA = djsonPOSTCNBA.toString();
				createPartitionIdPOSTCNBAFile(m_storageDirPath, m_arrayFolder, m_localDirPath, m_storageHandler, m_retryNumber, m_parallelNumber, detailsStrPOSTCNBA);
			}
			
			if (m_bigArray.size() > 0) {
				m_lastIndex = m_bigArray.size() - 1;
			}
			
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
	*/
	
	// This is given default access
	DataPartition(DataContext c, String dataId, String partitionUserRef, String storageDirPath, String arrayFolder, int pageSize, String localDirPath, IStorageHandler storageHandler, String createdBy) throws Exception {
		// This constructor is used for creating DataPartition
		// pass isNew as true
		if (null == c)
			throw new DScabiException("Data Context is null", "DPN.DPN.1");
		if (null == dataId)
			throw new DScabiException("Data Id is null", "DPN.DPN.2");
		if (null == partitionUserRef)
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
		if (null == createdBy)
			throw new DScabiException("CreatedBy is null", "DPN.DPN.9");
		if (createdBy.length() == 0)
			throw new DScabiException("CreatedBy is empty string", "DPN.DPN.9");
		
		try {
			// create DP purpose
			m_retryNumber = c.getRetryNumber();
			m_parallelNumber = c.getParallelNumber();
			m_actualArrayFolder = arrayFolder + "_R" + m_retryNumber + "_P" + m_parallelNumber + "_B" + (m_lastBigArray + 1);
			
			if (localDirPath.endsWith(File.separator))
				deleteLocalPartitionIfExists(localDirPath + m_actualArrayFolder);
			else
				deleteLocalPartitionIfExists(localDirPath + File.separator + m_actualArrayFolder);
			
			// Delete in storageDirPath is not needed as of 18-Feb-2017
			// cw if (storageDirPath.endsWith(File.separator))
			// cw 	storageHandler.deleteDirIfExists(storageDirPath + m_actualArrayFolder);
			// cw else
			// cw	storageHandler.deleteDirIfExists(storageDirPath + File.separator + m_actualArrayFolder);
			// OR
			// cw if (storageDirPath.endsWith(File.separator))
			// cw	storageHandler.deleteIfExists(storageDirPath + m_actualArrayFolder + File.separator + "meta_data" + File.separator + "page-0.dat");
			// cw else
			// cw 	storageHandler.deleteIfExists(storageDirPath + File.separator + m_actualArrayFolder + File.separator + "meta_data" + File.separator + "page-0.dat");

			// End create DP purpose
			
			m_isNew = true;
			m_context = c;
			m_storageDirPath = storageDirPath;
			m_arrayFolder = arrayFolder;
			m_localDirPath = localDirPath;
			m_storageHandler = storageHandler;
			m_dataId = dataId;
			m_partitionUserRef = partitionUserRef;
			m_pageSize = pageSize;
			m_createdBy = createdBy;
			
			// create DP purpose
			Instant inst = Instant.now();
			long nano = System.nanoTime();
			DMJson djsonPRECNBA = new DMJson();
			djsonPRECNBA.add("AppId", m_context.getAppId());
			djsonPRECNBA.add("DataId", m_dataId);
			djsonPRECNBA.add("SplitUnit", "" + m_context.getDU());
			djsonPRECNBA.add("PartitionUserRef", m_partitionUserRef);
			djsonPRECNBA.add("ArrayFolder", m_arrayFolder);
			djsonPRECNBA.add("RetryNumber", "" + m_retryNumber);
			djsonPRECNBA.add("ParallelNumber", "" + m_parallelNumber);
			djsonPRECNBA.add("PreCreateLastBigArray", "" + m_lastBigArray);				
			djsonPRECNBA.add("MaxRetry", "" + m_context.getMaxRetry());
			djsonPRECNBA.add("MaxParallel", "" + m_context.getMaxParallel());
			djsonPRECNBA.add("Timestamp", inst.toString());
			djsonPRECNBA.add("NanoTimestamp", "" + nano);
			djsonPRECNBA.add("CreatedBy", m_createdBy);
			djsonPRECNBA.add("Remarks", "Created");

			String detailsStrPRECNBA = djsonPRECNBA.toString();
			createPartitionIdPRECNBAFile(m_storageDirPath, m_arrayFolder, m_localDirPath, m_storageHandler, m_retryNumber, m_parallelNumber, detailsStrPRECNBA);
			// End create DP purpose
			
			m_bigArray = new BigArrayImpl(storageDirPath, m_actualArrayFolder, pageSize, localDirPath, m_storageHandler);			
			if (null == m_bigArray) {
				throw new DScabiException("big array is null", "DPN.DPN.10");
			} 

			m_currentBigArray++;
			m_bigArrayMap.put("" + m_currentBigArray, m_bigArray);

			// create DP purpose
			m_totalNumOfBigArrays++;				
			m_lastBigArray++;
			Instant inst2 = Instant.now();
			long nano2 = System.nanoTime();
			DMJson djsonPOSTCNBA = new DMJson();
			djsonPOSTCNBA.add("AppId", m_context.getAppId());
			djsonPOSTCNBA.add("DataId", m_dataId);
			djsonPOSTCNBA.add("SplitUnit", "" + m_context.getDU());
			djsonPOSTCNBA.add("PartitionUserRef", m_partitionUserRef);
			djsonPOSTCNBA.add("ArrayFolder", m_arrayFolder);
			djsonPOSTCNBA.add("RetryNumber", "" + m_retryNumber);
			djsonPOSTCNBA.add("ParallelNumber", "" + m_parallelNumber);
			djsonPOSTCNBA.add("PostCreateLastBigArray", "" + m_lastBigArray);
			djsonPOSTCNBA.add("MaxRetry", "" + m_context.getMaxRetry());
			djsonPOSTCNBA.add("MaxParallel", "" + m_context.getMaxParallel());
			djsonPOSTCNBA.add("Timestamp", inst2.toString());
			djsonPOSTCNBA.add("NanoTimestamp", "" + nano2);
			djsonPOSTCNBA.add("CreatedBy", m_createdBy);
			djsonPOSTCNBA.add("Remarks", "Created");
			
			String detailsStrPOSTCNBA = djsonPOSTCNBA.toString();
			createPartitionIdPOSTCNBAFile(m_storageDirPath, m_arrayFolder, m_localDirPath, m_storageHandler, m_retryNumber, m_parallelNumber, detailsStrPOSTCNBA);
			// End create DP purpose
			
			if (m_bigArray.size() > 0) {
				m_lastIndex = m_bigArray.size() - 1;
			}
			
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
	
	/* cw 19-Feb-2017
	// This is given default access
	DataPartition(DataContext c, String dataId, String partitionUserRef, String storageDirPath, String arrayFolder, int pageSize, String localDirPath, IStorageHandler storageHandler, boolean isNew, long retryNumber, long parallelNumber, long totalNumOfBigArrays) throws Exception {
		// This constructor is used for reading existing DataPartition
		// pass isNew as false
		if (null == c)
			throw new DScabiException("Data Context is null", "DPN.DPN.1");
		if (null == dataId)
			throw new DScabiException("Data Id is null", "DPN.DPN.2");
		if (null == partitionUserRef)
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
			if (isNew) {
				m_retryNumber = c.getRetryNumber();
				m_parallelNumber = c.getParallelNumber();
				m_actualArrayFolder = arrayFolder + "_R" + m_retryNumber + "_P" + m_parallelNumber + "_B" + (m_lastBigArray + 1);
				
				if (localDirPath.endsWith(File.separator))
					deleteLocalPartitionIfExists(localDirPath + m_actualArrayFolder);
				else
					deleteLocalPartitionIfExists(localDirPath + File.separator + m_actualArrayFolder);
				
				// Delete in storageDirPath is not needed as of 18-Feb-2017
				// cw if (storageDirPath.endsWith(File.separator))
				// cw	storageHandler.deleteDirIfExists(storageDirPath + m_actualArrayFolder);
				// cw else
				// cw	storageHandler.deleteDirIfExists(storageDirPath + File.separator + m_actualArrayFolder);
				// OR
				// cw if (storageDirPath.endsWith(File.separator))
				// cw	storageHandler.deleteIfExists(storageDirPath + m_actualArrayFolder + File.separator + "meta_data" + File.separator + "page-0.dat");
				// cw else
				// cw	storageHandler.deleteIfExists(storageDirPath + File.separator + m_actualArrayFolder + File.separator + "meta_data" + File.separator + "page-0.dat");
				
			} else {
				m_retryNumber = retryNumber;
				m_parallelNumber = parallelNumber;
				m_totalNumOfBigArrays = totalNumOfBigArrays;
				m_lastBigArray = totalNumOfBigArrays;
				m_actualArrayFolder = arrayFolder + "_R" + m_retryNumber + "_P" + m_parallelNumber + "_B" + (m_currentBigArray + 1);

				if (localDirPath.endsWith(File.separator))
					deleteLocalPartitionIfExists(localDirPath + m_actualArrayFolder);
				else
					deleteLocalPartitionIfExists(localDirPath + File.separator + m_actualArrayFolder);
			}
			
			m_isNew = isNew;
			m_context = c;
			m_storageDirPath = storageDirPath;
			m_arrayFolder = arrayFolder;
			m_localDirPath = localDirPath;
			m_storageHandler = storageHandler;
			m_dataId = dataId;
			m_partitionUserRef = partitionUserRef;
			m_pageSize = pageSize;
			
			if (isNew) {
				Instant inst = Instant.now();
				long nano = System.nanoTime();
				DMJson djsonPRECNBA = new DMJson();
				djsonPRECNBA.add("AppId", m_context.getAppId());
				djsonPRECNBA.add("DataId", m_dataId);
				djsonPRECNBA.add("SplitUnit", "" + m_context.getDU());
				djsonPRECNBA.add("PartitionUserRef", m_partitionUserRef);
				djsonPRECNBA.add("ArrayFolder", m_arrayFolder);
				djsonPRECNBA.add("RetryNumber", "" + m_retryNumber);
				djsonPRECNBA.add("ParallelNumber", "" + m_parallelNumber);
				djsonPRECNBA.add("PreCreateLastBigArray", "" + m_lastBigArray);				
				djsonPRECNBA.add("MaxRetry", "" + m_context.getMaxRetry());
				djsonPRECNBA.add("MaxParallel", "" + m_context.getMaxParallel());
				djsonPRECNBA.add("Timestamp", inst.toString());
				djsonPRECNBA.add("NanoTimestamp", "" + nano);
			
				String detailsStrPRECNBA = djsonPRECNBA.toString();
				createPartitionIdPRECNBAFile(m_storageDirPath, m_arrayFolder, m_localDirPath, m_storageHandler, m_retryNumber, m_parallelNumber, detailsStrPRECNBA);
			}
			
			m_bigArray = new BigArrayImpl(storageDirPath, m_actualArrayFolder, pageSize, localDirPath, m_storageHandler);			
			if (null == m_bigArray) {
				throw new DScabiException("big array is null", "DPN.DPN.10");
			} 
			
			m_currentBigArray++;
			m_bigArrayMap.put("" + m_currentBigArray, m_bigArray);

			if (isNew) {
				m_totalNumOfBigArrays++;
				m_lastBigArray++;
				Instant inst = Instant.now();
				long nano = System.nanoTime();
				DMJson djsonPOSTCNBA = new DMJson();
				djsonPOSTCNBA.add("AppId", m_context.getAppId());
				djsonPOSTCNBA.add("DataId", m_dataId);
				djsonPOSTCNBA.add("SplitUnit", "" + m_context.getDU());
				djsonPOSTCNBA.add("PartitionUserRef", m_partitionUserRef);
				djsonPOSTCNBA.add("ArrayFolder", m_arrayFolder);
				djsonPOSTCNBA.add("RetryNumber", "" + m_retryNumber);
				djsonPOSTCNBA.add("ParallelNumber", "" + m_parallelNumber);
				djsonPOSTCNBA.add("PostCreateLastBigArray", "" + m_lastBigArray);
				djsonPOSTCNBA.add("MaxRetry", "" + m_context.getMaxRetry());
				djsonPOSTCNBA.add("MaxParallel", "" + m_context.getMaxParallel());
				djsonPOSTCNBA.add("Timestamp", inst.toString());
				djsonPOSTCNBA.add("NanoTimestamp", "" + nano);
				
				String detailsStrPOSTCNBA = djsonPOSTCNBA.toString();
				createPartitionIdPOSTCNBAFile(m_storageDirPath, m_arrayFolder, m_localDirPath, m_storageHandler, m_retryNumber, m_parallelNumber, detailsStrPOSTCNBA);
			}
			
			if (m_bigArray.size() > 0) {
				m_lastIndex = m_bigArray.size() - 1;
			}
			
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
	*/
	//=====
	
	// This is given default access
	DataPartition(DataContext c, String dataId, String partitionUserRef, String storageDirPath, String arrayFolder, int pageSize, String localDirPath, IStorageHandler storageHandler, long retryNumber, long parallelNumber, long totalNumOfBigArrays, String createdBy) throws Exception {
		// This constructor is used for reading existing DataPartition
		// pass isNew as false
		if (null == c)
			throw new DScabiException("Data Context is null", "DPN.DPN.1");
		if (null == dataId)
			throw new DScabiException("Data Id is null", "DPN.DPN.2");
		if (null == partitionUserRef)
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
		if (null == createdBy)
			throw new DScabiException("CreatedBy is null", "DPN.DPN.9");
		if (createdBy.length() == 0)
			throw new DScabiException("CreatedBy is empty string", "DPN.DPN.9");
		
		try {
		    // read DP purpose
			m_retryNumber = retryNumber;
			m_parallelNumber = parallelNumber;
			m_totalNumOfBigArrays = totalNumOfBigArrays;
			m_lastBigArray = totalNumOfBigArrays;
			m_actualArrayFolder = arrayFolder + "_R" + m_retryNumber + "_P" + m_parallelNumber + "_B" + (m_currentBigArray + 1);

			if (localDirPath.endsWith(File.separator))
				deleteLocalPartitionIfExists(localDirPath + m_actualArrayFolder);
			else
				deleteLocalPartitionIfExists(localDirPath + File.separator + m_actualArrayFolder);
			// End read DP purpose
			
			m_isNew = false;
			m_context = c;
			m_storageDirPath = storageDirPath;
			m_arrayFolder = arrayFolder;
			m_localDirPath = localDirPath;
			m_storageHandler = storageHandler;
			m_dataId = dataId;
			m_partitionUserRef = partitionUserRef;
			m_pageSize = pageSize;
			m_createdBy = createdBy;
			
			m_bigArray = new BigArrayImpl(storageDirPath, m_actualArrayFolder, pageSize, localDirPath, m_storageHandler);			
			if (null == m_bigArray) {
				throw new DScabiException("big array is null", "DPN.DPN.10");
			} 
			
			m_currentBigArray++;
			m_bigArrayMap.put("" + m_currentBigArray, m_bigArray);

			if (m_bigArray.size() > 0) {
				m_lastIndex = m_bigArray.size() - 1;
			}
			
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
	
	public void close() throws Exception {
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
	
	public int operationsSuccessWithAppStatusCheck() throws Exception {
		// operationsSuccess() should be called only after all the data of this partition is 
		// completely and successfully appended()
		close();
		if (m_isNew) {
			// cw close();
			
			// if Appid.txt file doesn't exist throw exception
			// read app_line from Appid.txt file
			// if app_line is null throw exception
			// if app_line is empty throw exception
			// if app_line's status field is not "SUCCESS" throw exception
			
			/* cw
			int ind = m_arrayFolder.lastIndexOf("_");
			String appId = m_arrayFolder.substring(ind + 1);
			if (null == appId)
				throw new DScabiException("appId is null. Unable to get appId from m_arrayFolder : " + m_arrayFolder, "DPN.OSS.1");
			if (appId.length() == 0)
				throw new DScabiException("appId is empty. Unable to get appId from m_arrayFolder : " + m_arrayFolder, "DPN.OSS.1");
			*/
			
			String appId = m_context.getAppId();
			log.debug("operationsSuccessWithAppStatusCheck() appId : {}", appId);
			boolean check = isAppIdFileExists(m_storageDirPath, appId, m_storageHandler);
			if (check) {
				String appLine1 = getAppIdFileData(m_storageDirPath, appId, m_localDirPath, m_storageHandler);
				if (null == appLine1) {
					throw new DScabiException("Details in App Id file is null for appId : " + appId, "DPN.OSS.1");
				} else if (appLine1.length() == 0) {
					throw new DScabiException("Details in App Id file is empty for appId : " + appId, "DPN.OSS.1");
				} else {
					DMJson djsonApp1 = new DMJson(appLine1);
					String appStatus1 = djsonApp1.getString("Status");
					if (false == appStatus1.equalsIgnoreCase("INIT")) {
						throw new DScabiException("Status in App Id file is not INIT for appId : " + appId, "DPN.OSS.1");
					}
				}
			} else {
				throw new DScabiException("App Id file does not exist for appId : " + appId, "DPN.OSS.1");
			}
			
			logs.debug("operationsSuccessWithAppStatusCheck() Proceeding with isPartitionIdDeletedFileExists() check for partition : {}", m_arrayFolder);			   		
			boolean checkDeleted = isPartitionIdDeletedFileExists(m_storageDirPath, m_arrayFolder, m_storageHandler);
			if (checkDeleted) {
				throw new DScabiException("DataPartition for partition : " + m_arrayFolder + " is marked as deleted in Storage system", "DPN.OSS.1");
			}
			
			Instant inst = Instant.now();
			long nano = System.nanoTime();
			DMJson djsonRPCC1 = new DMJson();
			djsonRPCC1.add("AppId", m_context.getAppId());
			djsonRPCC1.add("DataId", m_dataId);
			djsonRPCC1.add("SplitUnit", "" + m_context.getDU());
			djsonRPCC1.add("PartitionUserRef", m_partitionUserRef);
			djsonRPCC1.add("ArrayFolder", m_arrayFolder);
			djsonRPCC1.add("RetryNumber", "" + m_retryNumber);
			djsonRPCC1.add("ParallelNumber", "" + m_parallelNumber);
			djsonRPCC1.add("TotalNumOfBigArrays", "" + m_totalNumOfBigArrays);
			djsonRPCC1.add("MaxRetry", "" + m_context.getMaxRetry());
			djsonRPCC1.add("MaxParallel", "" + m_context.getMaxParallel());
			djsonRPCC1.add("Timestamp", inst.toString());
			djsonRPCC1.add("NanoTimestamp", "" + nano);
			djsonRPCC1.add("Status", "SUCCESS");
			djsonRPCC1.add("CreatedBy", m_createdBy);
			djsonRPCC1.add("Remarks", "Created");
			
			// cw String detailsStrRPCC1 = m_retryNumber + ";SUCCESS;" + inst.toString() + ";" + nano + ";Created by : " + m_retryNumber;
			String detailsStrRPCC1 = djsonRPCC1.toString();
			createPartitionIdRPCCFile(m_storageDirPath, m_arrayFolder, m_localDirPath, m_storageHandler, m_retryNumber, m_parallelNumber, detailsStrRPCC1);

			// Again repeat the same checks for Appid.txt
			// if Appid.txt file doesn't exist throw exception
			// read app_line from Appid.txt file
			// if app_line is null throw exception
			// if app_line is empty throw exception
			// if app_line's status field is not "SUCCESS" throw exception
			boolean check2 = isAppIdFileExists(m_storageDirPath, appId, m_storageHandler);
			if (check2) {
				String appLine2 = getAppIdFileData(m_storageDirPath, appId, m_localDirPath, m_storageHandler);
				if (null == appLine2) {
					throw new DScabiException("Details in App Id file is null for appId : " + appId, "DPN.OSS.1");
				} else if (appLine2.length() == 0) {
					throw new DScabiException("Details in App Id file is empty for appId : " + appId, "DPN.OSS.1");
				} else {
					DMJson djsonApp2 = new DMJson(appLine2);
					String appStatus2 = djsonApp2.getString("Status");
					if (false == appStatus2.equalsIgnoreCase("INIT")) {
						throw new DScabiException("Status in App Id file is not INIT for appId : " + appId, "DPN.OSS.1");
					}
				}
			} else {
				throw new DScabiException("App Id file does not exist for appId : " + appId, "DPN.OSS.1");
			}
			
			logs.debug("operationsSuccessWithAppStatusCheck() Proceeding with isPartitionIdFileExists() check for partition : {}", m_arrayFolder);			   		
			boolean check3 = DataPartition.isPartitionIdFileExists(m_storageDirPath, m_arrayFolder, m_storageHandler);
			boolean proceed = false;	
			long retryNumber = -1;
			long parallelNumber = -1;
			
			if (check3) {
				logs.debug("operationsSuccessWithAppStatusCheck() Information only - Partition Id file exists for partition : {}, storageDirPath : {}", m_arrayFolder, m_storageDirPath);

				String detailsStrPid2 = DataPartition.getPartitionIdFileData(m_storageDirPath, m_arrayFolder, m_localDirPath, m_storageHandler);
				if (null == detailsStrPid2) {
					logs.debug("operationsSuccessWithAppStatusCheck() detailsStrPid2 is null for DataPartition for partition : " + m_arrayFolder + " Proceeding to create partition id file");
					proceed = true;
				} else {
					if (detailsStrPid2.length() == 0) {
						logs.debug("operationsSuccessWithAppStatusCheck() Details in partition id file : " + m_arrayFolder + ".txt is empty" + " Proceeding to create partition id file");					
						proceed = true;
					} else {	
						// cw String s[] = detailsStrPid2.split(";");
						// cw This number 5 should be consistent with how many ";" we write here in the end in operationsSuccess() method
						// cw if (s.length < 5) {					
						// cw 	logs.debug("operationsSuccessWithAppStatusCheck() Details in partition id file : " + m_arrayFolder + ".txt is not in correct format. Details is : " + detailsStr2 + " Proceeding to create partition id file");					
						// cw	proceed = true;		
						// cw } else {
						// cw 	retryNumber = Long.parseLong(s[0]);
						try {	
							// This try-catch is only for getting values from json
							// Will get exception if detailsStr is not in proper
							// json format
							DMJson djsonPid2 = new DMJson(detailsStrPid2);	
							retryNumber = djsonPid2.getLongOf("RetryNumber");
							parallelNumber = djsonPid2.getLongOf("ParallelNumber");
						} catch (Exception e) {
							log.debug("operationsSuccessWithAppStatusCheck() Details in partition id file : " + m_arrayFolder + ".txt is not in correct format. Details is : " + detailsStrPid2 + " Proceeding to create partition id file");					
							proceed = true;
						}
						if (false == proceed) {	
							if (parallelNumber == m_parallelNumber) {
								if (retryNumber == m_retryNumber) {
									throw new DScabiException("Partition id file is already having retry number same as this DU's retry number. m_retryNumber : " + m_retryNumber + " retryNumber : " + retryNumber + " m_parallelNumber : " + m_parallelNumber + " parallelNumber : " + parallelNumber + " m_arrayFolder : " + m_arrayFolder, "DPN.OSS.1");
								} else if (retryNumber < m_retryNumber) {
									log.debug("operationsSuccessWithAppStatusCheck() Information only - case retryNumber < m_retryNumber Proceeding to create partition id file");
									proceed = true;
								} else {
									// retryNumber > m_retryNumber
									throw new DScabiException("Partition id file is already having retry number greater than this DU's retry number. m_retryNumber : " + m_retryNumber + " retryNumber : " + retryNumber + " m_parallelNumber : " + m_parallelNumber + " parallelNumber : " + parallelNumber + " m_arrayFolder : " + m_arrayFolder, "DPN.OSS.1");
	
									/* cw 
									// This commented code below is old logic where partitionid_RNCC_<r>.txt file was created after creating partitionid.txt file
									log.debug("operationsSuccessWithAppStatusCheck() Information only - case retryNumber > m_retryNumber Proceeding to check if Partition Id RPCC file exists for partition : {}, storageDirPath : {}, retryNumber : {}, parallelNumber : {}", m_arrayFolder, m_storageDirPath, retryNumber, parallelNumber);
									boolean check4 = DataPartition.isPartitionIdRPCCFileExists(m_storageDirPath, m_arrayFolder, m_storageHandler, retryNumber, parallelNumber);
									if (check4) {
										logs.debug("operationsSuccessWithAppStatusCheck() Information only - Partition Id RPCC file exists for partition : {}, storageDirPath : {}, retryNumber : {}, parallelNumber : {}", m_arrayFolder, m_storageDirPath, retryNumber, parallelNumber);
										throw new DScabiException("Partition id file is already having retry number greater than this DU's retry number. m_retryNumber : " + m_retryNumber + " retryNumber : " + retryNumber + " m_parallelNumber : " + m_parallelNumber + " parallelNumber : " + parallelNumber + " m_arrayFolder : " + m_arrayFolder, "DPN.OSS.1");
									} else {
										logs.debug("operationsSuccessWithAppStatusCheck() Information only - Partition Id RPCC file does not exist for partition : {}, storageDirPath : {}, retryNumber : {} parallelNumber : {} Proceeding to create partition id file", m_arrayFolder, m_storageDirPath, retryNumber, parallelNumber);
										proceed = true;
									}	
									*/
								
								} 
							} else {
								log.debug("operationsSuccessWithAppStatusCheck() Information only - case parallelNumber Not Equal To m_parallelNumber Proceeding to create partition id file");
								proceed = true;
							}
						}
						// cw } // for else part
					}
				}
			} else {
				logs.debug("operationsSuccessWithAppStatusCheck() Information only - Partition Id file does not exist for partition : {}, storageDirPath : {} Proceeding to create partition id file", m_arrayFolder, m_storageDirPath);
				proceed = true;
			}				
			
			if (proceed) {
				logs.debug("operationsSuccessWithAppStatusCheck() Proceeding to create partition id file for partition : {}, storageDirPath : {}", m_arrayFolder, m_storageDirPath);
				Instant inst2 = Instant.now();
				long nano2 = System.nanoTime();
				DMJson djsonPid1 = new DMJson();
				djsonPid1.add("AppId", m_context.getAppId());
				djsonPid1.add("DataId", m_dataId);
				djsonPid1.add("SplitUnit", "" + m_context.getDU());
				djsonPid1.add("PartitionUserRef", m_partitionUserRef);
				djsonPid1.add("ArrayFolder", m_arrayFolder);
				djsonPid1.add("RetryNumber", "" + m_retryNumber);
				djsonPid1.add("ParallelNumber", "" + m_parallelNumber);
				djsonPid1.add("TotalNumOfBigArrays", "" + m_totalNumOfBigArrays);
				djsonPid1.add("MaxRetry", "" + m_context.getMaxRetry());
				djsonPid1.add("MaxParallel", "" + m_context.getMaxParallel());
				djsonPid1.add("Timestamp", inst2.toString());
				djsonPid1.add("NanoTimestamp", "" + nano2);
				djsonPid1.add("Status", "SUCCESS");
				djsonPid1.add("CreatedBy", m_createdBy);
				djsonPid1.add("Remarks", "Created");
				
				// cw String detailsStrPid1 = m_retryNumber + ";SUCCESS;" + inst2.toString() + ";" + nano2 + ";Created by : " + m_retryNumber;
				String detailsStrPid1 = djsonPid1.toString();
				createPartitionIdFile(m_storageDirPath, m_arrayFolder, m_localDirPath, m_storageHandler, detailsStrPid1);
			}
			
			m_isNew = false;
		}
		return 0;
	}
	
	public int operationsSuccess() throws Exception {
		// operationsSuccess() should be called only after all the data of this partition is 
		// completely and successfully appended()
		close();
		if (m_isNew) {
			// cw close();
		
			logs.debug("operationsSuccessWithAppStatusCheck() Proceeding with isPartitionIdDeletedFileExists() check for partition : {}", m_arrayFolder);			   		
			boolean checkDeleted = isPartitionIdDeletedFileExists(m_storageDirPath, m_arrayFolder, m_storageHandler);
			if (checkDeleted) {
				throw new DScabiException("DataPartition for partition : " + m_arrayFolder + " is marked as deleted in Storage system", "DPN.OSS.1");
			}
			
			Instant inst = Instant.now();
			long nano = System.nanoTime();
			DMJson djsonRPCC1 = new DMJson();
			djsonRPCC1.add("AppId", m_context.getAppId());
			djsonRPCC1.add("DataId", m_dataId);
			djsonRPCC1.add("SplitUnit", "" + m_context.getDU());
			djsonRPCC1.add("PartitionUserRef", m_partitionUserRef);
			djsonRPCC1.add("ArrayFolder", m_arrayFolder);
			djsonRPCC1.add("RetryNumber", "" + m_retryNumber);
			djsonRPCC1.add("ParallelNumber", "" + m_parallelNumber);
			djsonRPCC1.add("TotalNumOfBigArrays", "" + m_totalNumOfBigArrays);
			djsonRPCC1.add("MaxRetry", "" + m_context.getMaxRetry());
			djsonRPCC1.add("MaxParallel", "" + m_context.getMaxParallel());
			djsonRPCC1.add("Timestamp", inst.toString());
			djsonRPCC1.add("NanoTimestamp", "" + nano);
			djsonRPCC1.add("Status", "SUCCESS");
			djsonRPCC1.add("CreatedBy", m_createdBy);
			djsonRPCC1.add("Remarks", "Created");
			
			// cw String detailsStrRPCC1 = m_retryNumber + ";SUCCESS;" + inst.toString() + ";" + nano + ";Created by : " + m_retryNumber;
			String detailsStrRPCC1 = djsonRPCC1.toString();
			createPartitionIdRPCCFile(m_storageDirPath, m_arrayFolder, m_localDirPath, m_storageHandler, m_retryNumber, m_parallelNumber, detailsStrRPCC1);

			logs.debug("operationsSuccessWithAppStatusCheck() Proceeding with isPartitionIdFileExists() check for partition : {}", m_arrayFolder);			   		
			boolean check3 = DataPartition.isPartitionIdFileExists(m_storageDirPath, m_arrayFolder, m_storageHandler);
			boolean proceed = false;	
			long retryNumber = -1;
			long parallelNumber = -1;
			
			if (check3) {
				logs.debug("operationsSuccessWithAppStatusCheck() Information only - Partition Id file exists for partition : {}, storageDirPath : {}", m_arrayFolder, m_storageDirPath);

				String detailsStrPid2 = DataPartition.getPartitionIdFileData(m_storageDirPath, m_arrayFolder, m_localDirPath, m_storageHandler);
				if (null == detailsStrPid2) {
					logs.debug("operationsSuccessWithAppStatusCheck() detailsStrPid2 is null for DataPartition for partition : " + m_arrayFolder + " Proceeding to create partition id file");
					proceed = true;
				} else {
					if (detailsStrPid2.length() == 0) {
						logs.debug("operationsSuccessWithAppStatusCheck() Details in partition id file : " + m_arrayFolder + ".txt is empty" + " Proceeding to create partition id file");					
						proceed = true;
					} else {	
						// cw String s[] = detailsStrPid2.split(";");
						// cw This number 5 should be consistent with how many ";" we write here in the end in operationsSuccess() method
						// cw if (s.length < 5) {					
						// cw 	logs.debug("operationsSuccessWithAppStatusCheck() Details in partition id file : " + m_arrayFolder + ".txt is not in correct format. Details is : " + detailsStr2 + " Proceeding to create partition id file");					
						// cw	proceed = true;		
						// cw } else {
						// cw 	retryNumber = Long.parseLong(s[0]);
						try {	
							// This try-catch is only for getting values from json
							// Will get exception if detailsStr is not in proper
							// json format
							DMJson djsonPid2 = new DMJson(detailsStrPid2);	
							retryNumber = djsonPid2.getLongOf("RetryNumber");
							parallelNumber = djsonPid2.getLongOf("ParallelNumber");
						} catch (Exception e) {
							log.debug("operationsSuccessWithAppStatusCheck() Details in partition id file : " + m_arrayFolder + ".txt is not in correct format. Details is : " + detailsStrPid2 + " Proceeding to create partition id file");					
							proceed = true;
						}
						if (false == proceed) {
							if (parallelNumber == m_parallelNumber) {
								if (retryNumber == m_retryNumber) {
									throw new DScabiException("Partition id file is already having retry number same as this DU's retry number. m_retryNumber : " + m_retryNumber + " retryNumber : " + retryNumber + " m_parallelNumber : " + m_parallelNumber + " parallelNumber : " + parallelNumber + " m_arrayFolder : " + m_arrayFolder, "DPN.OSS.1");
								} else if (retryNumber < m_retryNumber) {
									log.debug("operationsSuccessWithAppStatusCheck() Information only - case retryNumber < m_retryNumber Proceeding to create partition id file");
									proceed = true;
								} else {
									// retryNumber > m_retryNumber
									throw new DScabiException("Partition id file is already having retry number greater than this DU's retry number. m_retryNumber : " + m_retryNumber + " retryNumber : " + retryNumber + " m_parallelNumber : " + m_parallelNumber + " parallelNumber : " + parallelNumber + " m_arrayFolder : " + m_arrayFolder, "DPN.OSS.1");

									/* cw 
									// This commented code below is old logic where partitionid_RNCC_<r>.txt file was created after creating partitionid.txt file
									log.debug("operationsSuccessWithAppStatusCheck() Information only - case retryNumber > m_retryNumber Proceeding to check if Partition Id RPCC file exists for partition : {}, storageDirPath : {}, retryNumber : {}, parallelNumber : {}", m_arrayFolder, m_storageDirPath, retryNumber, parallelNumber);
									boolean check4 = DataPartition.isPartitionIdRPCCFileExists(m_storageDirPath, m_arrayFolder, m_storageHandler, retryNumber, parallelNumber);
									if (check4) {
										logs.debug("operationsSuccessWithAppStatusCheck() Information only - Partition Id RPCC file exists for partition : {}, storageDirPath : {}, retryNumber : {}, parallelNumber : {}", m_arrayFolder, m_storageDirPath, retryNumber, parallelNumber);
										throw new DScabiException("Partition id file is already having retry number greater than this DU's retry number. m_retryNumber : " + m_retryNumber + " retryNumber : " + retryNumber + " m_parallelNumber : " + m_parallelNumber + " parallelNumber : " + parallelNumber + " m_arrayFolder : " + m_arrayFolder, "DPN.OSS.1");
									} else {
										logs.debug("operationsSuccessWithAppStatusCheck() Information only - Partition Id RPCC file does not exist for partition : {}, storageDirPath : {}, retryNumber : {} parallelNumber : {} Proceeding to create partition id file", m_arrayFolder, m_storageDirPath, retryNumber, parallelNumber);
										proceed = true;
									}	
									*/
								} 
							} else {
								log.debug("operationsSuccessWithAppStatusCheck() Information only - case parallelNumber Not Equal To m_parallelNumber Proceeding to create partition id file");
								proceed = true;
							}
						}
						// cw } // for else part
					}
				}
			} else {
				logs.debug("operationsSuccessWithAppStatusCheck() Information only - Partition Id file does not exist for partition : {}, storageDirPath : {} Proceeding to create partition id file", m_arrayFolder, m_storageDirPath);
				proceed = true;
			}				
			
			if (proceed) {
				logs.debug("operationsSuccessWithAppStatusCheck() Proceeding to create partition id file for partition : {}, storageDirPath : {}", m_arrayFolder, m_storageDirPath);
				Instant inst2 = Instant.now();
				long nano2 = System.nanoTime();
				DMJson djsonPid1 = new DMJson();
				djsonPid1.add("AppId", m_context.getAppId());
				djsonPid1.add("DataId", m_dataId);
				djsonPid1.add("SplitUnit", "" + m_context.getDU());
				djsonPid1.add("PartitionUserRef", m_partitionUserRef);
				djsonPid1.add("ArrayFolder", m_arrayFolder);
				djsonPid1.add("RetryNumber", "" + m_retryNumber);
				djsonPid1.add("ParallelNumber", "" + m_parallelNumber);
				djsonPid1.add("TotalNumOfBigArrays", "" + m_totalNumOfBigArrays);
				djsonPid1.add("MaxRetry", "" + m_context.getMaxRetry());
				djsonPid1.add("MaxParallel", "" + m_context.getMaxParallel());
				djsonPid1.add("Timestamp", inst2.toString());
				djsonPid1.add("NanoTimestamp", "" + nano2);
				djsonPid1.add("Status", "SUCCESS");
				djsonPid1.add("CreatedBy", m_createdBy);
				djsonPid1.add("Remarks", "Created");
				
				// cw String detailsStrPid1 = m_retryNumber + ";SUCCESS;" + inst2.toString() + ";" + nano2 + ";Created by : " + m_retryNumber;
				String detailsStrPid1 = djsonPid1.toString();
				createPartitionIdFile(m_storageDirPath, m_arrayFolder, m_localDirPath, m_storageHandler, detailsStrPid1);
			}
			
			m_isNew = false;
		}
		return 0;
	}
	
	private void closeArrayOnly() throws Exception {
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
		Iterable<String> values = m_shuffle.groupByValues(m_dataElement, m_context);
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
		Iterable<String> values = m_shuffle.groupByValues(m_dataElement, m_context);
		long hash = hash(values);
		return hash;
	}
	
	public boolean isHashElementBelongsToSU(long index, long tu, long su) throws Exception {
		gc();
		byte[] bytea = m_bigArray.get(index);
		m_dataElement.set(bytea);
		Iterable<String> values = m_shuffle.groupByValues(m_dataElement, m_context);
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
		Iterable<String> values = m_shuffle.groupByValues(m_dataElement, m_context);
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
	
	public byte[] nextBytes() throws Exception {

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
	
	public String nextString() throws Exception {

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
	
	public String nextUnicodeString() throws Exception {

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
	
	public DataElement next() throws Exception {

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

	public int next(DataElement e) throws Exception {

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
	
	public Dson nextDson() throws Exception {

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
	
	public int nextDson(Dson dson) throws Exception {

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
	
	public <T> T next(Class<T> t) throws Exception {

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
	
	public int nextInt() throws Exception {

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
	
	public long nextLong() throws Exception {

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
	
	public float nextFloat() throws Exception {

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
	
	public double nextDouble() throws Exception {

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
	
	public boolean nextBoolean() throws Exception {

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

	public byte[] getBytes(long index) throws Exception {

		gc();
		setOperationTypeGet();		
		
		byte[] bytea = m_bigArray.get(index);
		return bytea;
	}
	
	public String getString(long index) throws Exception {

		gc();
		setOperationTypeGet();		
		
		byte[] bytea = m_bigArray.get(index);
		return new String(bytea, M_DEFAULT_ENCODING);
	}
	
	public DataElement get(long index) throws Exception {

		gc();
		setOperationTypeGet();		
		
		byte[] bytea = m_bigArray.get(index);
		return new DataElement(bytea);
	}

	public byte[] getBytes() throws Exception {

		gc();
		setOperationTypeGet();		
		
		if (m_currentIndex <= m_lastIndex) {
			byte[] bytea = m_bigArray.get(m_currentIndex);
			return bytea;
		}	
		else
			return null;
	}
	
	public String getString() throws Exception {

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

	public String getUnicodeString() throws Exception {

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

	public DataElement get() throws Exception {

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
	
	public int get(DataElement e) throws Exception {

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
	
	public Dson getDson() throws Exception {

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
	
	public int getDson(Dson dson) throws Exception {

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
	
	public <T> T get(Class<T> t) throws Exception {
		
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
	
	public int getInt() throws Exception {
		
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
	
	public long getLong() throws Exception {
		
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
	
	public float getFloat() throws Exception {
		
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
	
	public double getDouble() throws Exception {
		
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
	
	public boolean getBoolean() throws Exception {
		
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
	
	public String getField(String fieldName) throws Exception {
		
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

	public int getIntField(String fieldName) throws Exception {
		
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
	
	public long getLongField(String fieldName) throws Exception {
		
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
	
	public double getDoubleField(String fieldName) throws Exception {
		
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
	
	public boolean getBooleanField(String fieldName) throws Exception {
		
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

	public void append(byte[] bytea) throws Exception {
		gc();
		setOperationTypeAppend();
		
		m_bigArray.append(bytea);
		m_lastIndex++;
	}
	
	public void append(String s) throws Exception {
		gc();
		setOperationTypeAppend();
		
		byte[] bytea = s.getBytes(M_DEFAULT_ENCODING); // so other 2-byte Unicode languages are not supported
		m_bigArray.append(bytea);	
		m_lastIndex++;
	}
	
	public void appendUnicode(String s) throws Exception {
		gc();
		setOperationTypeAppend();
		
		byte[] bytea = s.getBytes();
		m_bigArray.append(bytea);
		m_lastIndex++;
	}
	
	public void append(DataElement e) throws Exception {
		gc();
		setOperationTypeAppend();
		
		byte[] bytea = e.getBytes();
		m_bigArray.append(bytea);	
		m_lastIndex++;
	}
	
	public void append(Dson dson) throws Exception {
		gc();
		setOperationTypeAppend();
		
		byte[] bytea = dson.toString().getBytes(M_DEFAULT_ENCODING); // so other 2-byte Unicode languages are not supported
		m_bigArray.append(bytea);	
		m_lastIndex++;
	}
		
	public <T> T append(T tobj) throws Exception {
		gc();
		setOperationTypeAppend();
		
		byte[] bytea = m_objectMapper.writeValueAsBytes(tobj);
		m_bigArray.append(bytea);
		m_lastIndex++;
		
		return tobj;
	}
	
	// append(...) for primary data types and related
	
	public void append(int value) throws Exception {
		gc();
		setOperationTypeAppend();
		
		String s = "" + value;
		byte[] bytea = s.getBytes(M_DEFAULT_ENCODING); // so other 2-byte Unicode languages are not supported
		m_bigArray.append(bytea);	
		m_lastIndex++;
	}
	
	public void append(long value) throws Exception {
		gc();
		setOperationTypeAppend();
		
		String s = "" + value;
		byte[] bytea = s.getBytes(M_DEFAULT_ENCODING); // so other 2-byte Unicode languages are not supported
		m_bigArray.append(bytea);	
		m_lastIndex++;
	}
	
	public void append(float value) throws Exception {
		gc();
		setOperationTypeAppend();
		
		String s = "" + value;
		byte[] bytea = s.getBytes(M_DEFAULT_ENCODING); // so other 2-byte Unicode languages are not supported
		m_bigArray.append(bytea);	
		m_lastIndex++;
	}

	public void append(double value) throws Exception {
		gc();
		setOperationTypeAppend();
		
		String s = "" + value;
		byte[] bytea = s.getBytes(M_DEFAULT_ENCODING); // so other 2-byte Unicode languages are not supported
		m_bigArray.append(bytea);	
		m_lastIndex++;
	}

	public void append(boolean value) throws Exception {
		gc();
		setOperationTypeAppend();
		
		String s = "" + value;
		byte[] bytea = s.getBytes(M_DEFAULT_ENCODING); // so other 2-byte Unicode languages are not supported
		m_bigArray.append(bytea);	
		m_lastIndex++;
	}

	public void append(Integer value) throws Exception {
		gc();
		setOperationTypeAppend();
		
		String s = "" + value;
		byte[] bytea = s.getBytes(M_DEFAULT_ENCODING); // so other 2-byte Unicode languages are not supported
		m_bigArray.append(bytea);	
		m_lastIndex++;
	}

	public void append(Long value) throws Exception {
		gc();
		setOperationTypeAppend();
		
		String s = "" + value;
		byte[] bytea = s.getBytes(M_DEFAULT_ENCODING); // so other 2-byte Unicode languages are not supported
		m_bigArray.append(bytea);	
		m_lastIndex++;
	}

	public void append(Float value) throws Exception {
		gc();
		setOperationTypeAppend();
		
		String s = "" + value;
		byte[] bytea = s.getBytes(M_DEFAULT_ENCODING); // so other 2-byte Unicode languages are not supported
		m_bigArray.append(bytea);	
		m_lastIndex++;
	}

	public void append(Double value) throws Exception {
		gc();
		setOperationTypeAppend();
		
		String s = "" + value;
		byte[] bytea = s.getBytes(M_DEFAULT_ENCODING); // so other 2-byte Unicode languages are not supported
		m_bigArray.append(bytea);	
		m_lastIndex++;
	}

	public void append(Boolean value) throws Exception {
		gc();
		setOperationTypeAppend();
		
		String s = "" + value;
		byte[] bytea = s.getBytes(M_DEFAULT_ENCODING); // so other 2-byte Unicode languages are not supported
		m_bigArray.append(bytea);	
		m_lastIndex++;
	}

	// appendField(...) and appendRow() methods
	
	public int appendRow() throws Exception {
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

	public void removeAll() throws Exception {
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
			values = m_shuffle.groupByValues(m_dataElement, m_context);
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
	
	public int exportToFile(String filePath) throws Exception {
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
	
	public int importFromFile(String filePath) throws Exception {
		
		if (false == m_isNew)
			throw new DScabiException("Data Partition cannot be modified, arrayFolder : " + m_arrayFolder + " retryNumber : " + m_retryNumber + " parallelNumber : " + m_parallelNumber + " storageDirPath " + m_storageDirPath, "DPN.IMF.1");
		
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
		deletePartitionWithoutDeletedFile();
		
		// cw m_bigArray = new BigArrayImpl(m_storageDirPath, m_arrayFolder, m_pageSize, m_localDirPath, m_storageHandler);
		
		m_totalNumOfBigArrays = 0;
		m_bigArrayMap = new HashMap<String, IBigArray>();
		m_currentBigArray = 0;
		m_lastBigArray = 0;
		
		m_actualArrayFolder = m_arrayFolder + "_R" + m_retryNumber + "_P" + m_parallelNumber + "_B" + (m_lastBigArray + 1);
		
		if (m_localDirPath.endsWith(File.separator))
			deleteLocalPartitionIfExists(m_localDirPath + m_actualArrayFolder);
		else
			deleteLocalPartitionIfExists(m_localDirPath + File.separator + m_actualArrayFolder);
		
		Instant inst = Instant.now();
		long nano = System.nanoTime();
		DMJson djsonPRECNBA = new DMJson();
		djsonPRECNBA.add("AppId", m_context.getAppId());
		djsonPRECNBA.add("DataId", m_dataId);
		djsonPRECNBA.add("SplitUnit", "" + m_context.getDU());
		djsonPRECNBA.add("PartitionUserRef", m_partitionUserRef);
		djsonPRECNBA.add("ArrayFolder", m_arrayFolder);
		djsonPRECNBA.add("RetryNumber", "" + m_retryNumber);
		djsonPRECNBA.add("ParallelNumber", "" + m_parallelNumber);
		djsonPRECNBA.add("PreCreateLastBigArray", "" + m_lastBigArray);				
		djsonPRECNBA.add("MaxRetry", "" + m_context.getMaxRetry());
		djsonPRECNBA.add("MaxParallel", "" + m_context.getMaxParallel());
		djsonPRECNBA.add("Timestamp", inst.toString());
		djsonPRECNBA.add("NanoTimestamp", "" + nano);
		djsonPRECNBA.add("CreatedBy", m_createdBy);
		djsonPRECNBA.add("Remarks", "Created");
		
		String detailsStrPRECNBA = djsonPRECNBA.toString();
		createPartitionIdPRECNBAFile(m_storageDirPath, m_arrayFolder, m_localDirPath, m_storageHandler, m_retryNumber, m_parallelNumber, detailsStrPRECNBA);
		
		m_bigArray = new BigArrayImpl(m_storageDirPath, m_actualArrayFolder, m_pageSize, m_localDirPath, m_storageHandler);		

		m_currentBigArray++;
		m_bigArrayMap.put("" + m_currentBigArray, m_bigArray);

		m_totalNumOfBigArrays++;				
		m_lastBigArray++;
		Instant inst2 = Instant.now();
		long nano2 = System.nanoTime();
		DMJson djsonPOSTCNBA = new DMJson();
		djsonPOSTCNBA.add("AppId", m_context.getAppId());
		djsonPOSTCNBA.add("DataId", m_dataId);
		djsonPOSTCNBA.add("SplitUnit", "" + m_context.getDU());
		djsonPOSTCNBA.add("PartitionUserRef", m_partitionUserRef);
		djsonPOSTCNBA.add("ArrayFolder", m_arrayFolder);
		djsonPOSTCNBA.add("RetryNumber", "" + m_retryNumber);
		djsonPOSTCNBA.add("ParallelNumber", "" + m_parallelNumber);
		djsonPOSTCNBA.add("PostCreateLastBigArray", "" + m_lastBigArray);
		djsonPOSTCNBA.add("MaxRetry", "" + m_context.getMaxRetry());
		djsonPOSTCNBA.add("MaxParallel", "" + m_context.getMaxParallel());
		djsonPOSTCNBA.add("Timestamp", inst2.toString());
		djsonPOSTCNBA.add("NanoTimestamp", "" + nano2);
		djsonPOSTCNBA.add("CreatedBy", m_createdBy);
		djsonPOSTCNBA.add("Remarks", "Created");
		
		String detailsStrPOSTCNBA = djsonPOSTCNBA.toString();
		createPartitionIdPOSTCNBAFile(m_storageDirPath, m_arrayFolder, m_localDirPath, m_storageHandler, m_retryNumber, m_parallelNumber, detailsStrPOSTCNBA);
		
		// cw createPartitionIdFile(m_storageDirPath, m_arrayFolder, m_localDirPath, m_storageHandler);
		m_isNew = true;
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
		
		if (s.length() == 0)
			s = "Data Partition is empty. partitionUserRef : " + m_partitionUserRef + " Array Folder : " + m_arrayFolder;
		else
			s = s.substring(0, s.length() - 1);
		return s;
	}
}
