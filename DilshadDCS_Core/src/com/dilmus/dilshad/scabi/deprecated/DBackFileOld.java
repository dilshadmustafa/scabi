/**
 * @author Dilshad Mustafa
 * Copyright (c) Dilshad Mustafa
 * All Rights Reserved.
 * @since 11-Feb-2016
 * File Name : DBackFileOld.java
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

package com.dilmus.dilshad.scabi.deprecated;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import java.text.ParseException;
import java.text.SimpleDateFormat;

import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.TimeZone;

import org.bson.BasicBSONObject;
//import org.bson.Document;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dilmus.dilshad.scabi.common.DScabiException;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.WriteResult;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.gridfs.GridFSBucket;
import com.mongodb.client.gridfs.GridFSBuckets;
import com.mongodb.client.gridfs.GridFSDownloadStream;
import com.mongodb.client.gridfs.GridFSUploadStream;
import com.mongodb.client.gridfs.model.GridFSDownloadByNameOptions;
import com.mongodb.client.gridfs.model.GridFSUploadOptions;
import com.mongodb.gridfs.GridFS;

/**
 * @author Dilshad Mustafa
 *
 */
public class DBackFileOld {

	final static Logger log = LoggerFactory.getLogger(DBackFileOld.class);

	private DDBOld m_ddb = null;
	private MongoDatabase m_mongodb = null;
    private GridFSBucket m_gridFSBucket = null;
    private GridFSUploadOptions m_options = null;
    
	private int m_chunkSize = 1024*1024; // Bytes
	private int m_bufferSize = 64*1024*1024; // Bytes
    private DBCollection m_table = null;
        
	public DBackFileOld(DDBOld ddb) {
		m_ddb = ddb;
		m_mongodb = ddb.getDatabase();
		m_chunkSize = 1024*1024;
		m_bufferSize = 64*1024*1024;

	   	m_table = m_ddb.getDB().getCollection("fs.files");
		
	   	// Don't use GridFS() class, raises ClassCastException in with DBCursor logic m_gfs = new GridFS(m_ddb.getDB());

		m_gridFSBucket = GridFSBuckets.create(m_mongodb);
	        
        // Create some custom options
		m_options = new GridFSUploadOptions()
                .chunkSizeBytes(m_chunkSize);
		
		/* Reference how to add meta-data MongoDB/GridFS specific way
		Document doc = new Document();
		doc.append("Type", "File");
		doc.append("ContentType", "File");
			
		m_options = new GridFSUploadOptions()
                .chunkSizeBytes(m_chunkSize)
                .metadata(doc);
	    */  
	   	
	}
	
	public int close() {
		m_ddb = null;
		m_mongodb = null;
	    m_gridFSBucket = null;
	    m_options = null;
		m_table = null;
		
		return 0;
	}
	
	public int setBufferSize(int bufferSize) {
		m_bufferSize = bufferSize;
		return 0;
	}
	
	public int updateMetaData(String fileName, ObjectId fileID, String type, String contentType) throws IOException, DScabiException, ParseException {
		int n = 0;
		String uploadDate = null;
		Date datefromDB = null;
		
		BasicDBObject documentWhere = new BasicDBObject();
    	documentWhere.put("_id", fileID);

	   	DBCursor cursorExist = m_table.find(documentWhere);
    	n = cursorExist.count();
    	if (1 == n) {
			log.debug("updateMetaData() Inside 1 == n");
			while (cursorExist.hasNext()) {
		    	DBObject ob = cursorExist.next();
		    	log.debug("updateMetaData() result from ob {}", ob.toString());
		    	//datefromDB = (String) ((BasicBSONObject) ob).getString("uploadDate");
		    	datefromDB = ((BasicBSONObject)ob).getDate("uploadDate");
		    	if (null == datefromDB) {
					throw new DScabiException("updateMetaData() Unable to get uploadDate for file : " + fileName + " fileID : " + fileID.toHexString(), "DBF.UMD.1");
				}
				log.debug("datefromDB : {}", datefromDB);
			
			}

    	}  else if (0 == n) {
			log.debug("updateMetaData() No matches for file : " + fileName + " fileID : " + fileID.toHexString());
			throw new DScabiException("updateMetaData() No matches for file : " + fileName + " fileID : " + fileID.toHexString(), "DBF.UMD.2");
    	} else {
			log.debug("updateMetaData() Multiple matches for file : " + fileName + " fileID : " + fileID.toHexString());
			throw new DScabiException("updateMetaData() Multiple matches for file : " + fileName + " fileID : " + fileID.toHexString(), "DBF.UMD.3");
    	}

    	
        Date date = new Date();
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmssSSS");
        dateFormat.setTimeZone(TimeZone.getTimeZone("ISO"));
        String putClientDateTime = dateFormat.format(date);
        // To parse from string : Date date2 = dateFormat.parse(putDateTime);
        // Uses java.time java 8 : ZonedDateTime now = ZonedDateTime.now( ZoneOffset.UTC );	        
        String millisTime = "" + System.currentTimeMillis();
        String nanoTime = "" + System.nanoTime();

        /* If datefromDB is String
        SimpleDateFormat dateFormatFromDB = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        dateFormatFromDB.setTimeZone(TimeZone.getTimeZone("ISO"));

		CharSequence cs1 = "T";
		CharSequence cs2 = "Z";
		String s1 = datefromDB.replace(cs1, " ");
		String s2 = s1.replace(cs2, "");

		Date date2 = dateFormatFromDB.parse(s2);
        uploadDate = dateFormat.format(date2);
        */
        
        uploadDate = dateFormat.format(datefromDB);
        log.debug("uploadDate : {}", uploadDate);
        
        BasicDBObject documentUpdate = new BasicDBObject();
        documentUpdate.append("PutFileName", fileName);
        documentUpdate.append("PutServerFileID", fileID.toHexString());
        documentUpdate.append("PutServerUploadDateTime", uploadDate);
        documentUpdate.append("PutType", type);
		documentUpdate.append("PutContentType", contentType);
		documentUpdate.append("PutClientDateTime", putClientDateTime);
		documentUpdate.append("PutClientDateTimeInMillis", millisTime);
		documentUpdate.append("PutClientDateTimeInNano", nanoTime);	
    	documentUpdate.append("PutStatus", "Completed");
    	documentUpdate.append("PutLatestNumber", "1");
	
	   	BasicDBObject updateObj = new BasicDBObject();
	   	updateObj.put("$set", documentUpdate);

	   	WriteResult result = m_table.update(documentWhere, updateObj);
	   	if (1 != result.getN())
			throw new DScabiException("Update meta data failed for file : " + fileName + " fileID : " + fileID.toHexString(), "DBF.UMD.4");
		
        handlePreviousVersions(fileName, fileID.toHexString(), uploadDate);
	   	
	   	return result.getN();
	
	}

	private int handlePreviousVersions(String fileName, String strFileID, String strPutServerUploadDateTime) throws IOException, DScabiException {
		int m = 0;
		int n = 0;
		
		// It is better to call this only after meta data is updated for currently uploaded file
		// This will skip checking for given input strFileID, file ID of currently uploaded file
		removeFilesIncompleteMetaData(fileName, strFileID);
		
        BasicDBObject documentFind = new BasicDBObject();
    	documentFind.put("PutFileName", fileName);
        documentFind.append("PutServerFileID", strFileID);
    	documentFind.append("PutStatus", "Completed");
    	documentFind.append("PutLatestNumber", "1");

	   	DBCursor cursor = m_table.find(documentFind);
    	m = cursor.count();
		
    	if (1 == m) {
			log.debug("handlePreviousVersions() Inside 1 == n");
    	}  else if (0 == m) {
			log.debug("handlePreviousVersions() No matches for file : " + fileName + " strFileID : " + strFileID);
			throw new DScabiException("handlePreviousVersions() No matches for file : " + fileName + " strFileID : " + strFileID, "DBF.HPV.1");
    	} else {
			log.debug("handlePreviousVersions() Multiple matches for file : " + fileName + " strFileID : " + strFileID);
			throw new DScabiException("handlePreviousVersions() Multiple matches for file : " + fileName + " strFileID : " + strFileID, "DBF.HPV.2");
    	}
		
        BasicDBObject documentQuery = new BasicDBObject();
    	documentQuery.put("PutFileName", fileName);
        documentQuery.append("PutStatus", "Completed");

	   	DBCursor cursorExist = m_table.find(documentQuery);
    	n = cursorExist.count();
    	if (1 == n) {
			log.debug("handlePreviousVersions() Information only : Inside 1 == n. Only one file / current file is found. No previous versions for file : " + fileName + " with PutStatus=Completed");
			return 0;
    	}  else if (0 == n) {
			log.debug("handlePreviousVersions() No matches for file : " + fileName + " with PutStatus=Completed");
			throw new DScabiException("handlePreviousVersions()() No matches for file : " + fileName + " with PutStatus=Completed", "DBF.HPV.3");
    	} else {
			long lf1 = Long.parseLong(strPutServerUploadDateTime);
    		while (cursorExist.hasNext()) {
		    	DBObject ob = cursorExist.next();
		    	log.debug("handlePreviousVersions() result from ob {}", ob.toString());
		    	
		    	String fid = (String) ((BasicBSONObject) ob).getString("PutServerFileID");
				if (null == fid) {
					throw new DScabiException("PutServerFileID is missing for one version of file : " + fileName, "DBF.HPV.4");
				}
				/* Don't use. It should be based on date-time and not on file ID
				if (f.equals(strFileID)) {
					// proceed with other versions
					continue;
				}
				*/
		    	String f = (String) ((BasicBSONObject) ob).getString("PutServerUploadDateTime");
				if (null == f) {
					throw new DScabiException("PutServerUploadDateTime is missing for one version of file : " + fileName + " file ID : " + fid, "DBF.HPV.5");
				}
				String f2 = (String) ((BasicBSONObject) ob).getString("PutLatestNumber");
				if (null == f2) {
					throw new DScabiException("PutLatestNumber is missing for one version of file : " + fileName + " file ID : " + fid, "DBF.HPV.6");
				}
				if (f.equals(strPutServerUploadDateTime) && f2.equals("1")) {
					// proceed with other versions
					continue;
				}
				long lf2 = Long.parseLong(f);
				if (lf1 < lf2 && f2.equals("1")) {
					// proceed with other versions
					continue;
				}
				if (f2.equals("1")) {
					// all file entries here have PutServerUploadDateTime < strPutServerUploadDateTime
					// there can be multiple previous versions with PutLatestNumber=1
					BasicDBObject documentWhere = new BasicDBObject();
			    	documentWhere.put("PutServerFileID", fid);

			        BasicDBObject documentUpdate = new BasicDBObject();
			        documentUpdate.append("PutLatestNumber", "2");

				   	BasicDBObject updateObj = new BasicDBObject();
				   	updateObj.put("$set", documentUpdate);
				   	// there should be only one entry for file ID fid
				   	WriteResult result = m_table.update(documentWhere, updateObj);
				   	if (result.getN() <= 0)
						throw new DScabiException("Update meta data to PutLatestNumber=2 failed for file : " + fileName + " file ID : " + fid, "DBF.HPV.7");
				} else {
					// remove all other versions
					m_gridFSBucket.delete(new ObjectId(fid));
				}
				
			}
    	}
    	return 0;
	}

	private int removeFilesIncompleteMetaData(String fileName, String strFileID) {
		int n = 0;
		Set<String> stMetaKeys = new HashSet<String>();
		stMetaKeys.add("PutFileName");
		stMetaKeys.add("PutServerFileID");
		stMetaKeys.add("PutServerUploadDateTime");
		stMetaKeys.add("PutType");
		stMetaKeys.add("PutContentType");
		stMetaKeys.add("PutClientDateTime");
		stMetaKeys.add("PutClientDateTimeInMillis");
		stMetaKeys.add("PutClientDateTimeInNano");	
		stMetaKeys.add("PutStatus");
		stMetaKeys.add("PutLatestNumber");

        BasicDBObject documentQuery = new BasicDBObject();
        // "filename" is MongoDB/GridFS specific meta data name inside fs.files collection for each file
        documentQuery.put("filename", fileName); 

	   	DBCursor cursorExist = m_table.find(documentQuery);
    	n = cursorExist.count();
    	if (0 == n) {
			log.debug("removeFilesIncompleteMetaData() Information only : No file found for file : " + fileName);
			return 0;
    	} else {
    		while (cursorExist.hasNext()) {
		    	DBObject ob = cursorExist.next();
		    	log.debug("removeFilesIncompleteMetaData() result from ob {}", ob.toString());
	    		// "_id" is MongoDB/GridFS specific meta data name inside fs.files collection for each file
		    	ObjectId oid = ((BasicBSONObject) ob).getObjectId("_id");
				if (null == oid) {
					// what's the use in throwing exception here? throw new DScabiException("_id is missing for file : " + fileName, "DBF.RFI.1");
					// let it continue to cleanup as much as possible
					continue;
				}
		    	if (oid.toHexString().equals(strFileID)) {
			    	log.debug("removeFilesIncompleteMetaData() Information only : skipping given input file ID : {}", strFileID);
			    	continue;
		    	}
		    	Set<String> st = ob.keySet();
		    	if (st.containsAll(stMetaKeys)) {
		    		continue;
		    	} else {
					// remove file
					m_gridFSBucket.delete(oid);

		    	}
    		}
    	}
		return 0;
	}

	public int removeAllFilesIncompleteMetaData() {
		int n = 0;
		Set<String> stMetaKeys = new HashSet<String>();
		stMetaKeys.add("PutFileName");
		stMetaKeys.add("PutServerFileID");
		stMetaKeys.add("PutServerUploadDateTime");
		stMetaKeys.add("PutType");
		stMetaKeys.add("PutContentType");
		stMetaKeys.add("PutClientDateTime");
		stMetaKeys.add("PutClientDateTimeInMillis");
		stMetaKeys.add("PutClientDateTimeInNano");	
		stMetaKeys.add("PutStatus");
		stMetaKeys.add("PutLatestNumber");

	   	DBCursor cursorExist = m_table.find();
	   	n = cursorExist.count();
    	if (0 == n) {
			log.debug("removeAllFilesIncompleteMetaData() Information only : No file found");
			return 0;
    	} else {
    		while (cursorExist.hasNext()) {
		    	DBObject ob = cursorExist.next();
		    	log.debug("removeAllFilesIncompleteMetaData() result from ob {}", ob.toString());
	    		// "_id" is MongoDB/GridFS specific meta data name inside fs.files collection for each file
		    	ObjectId oid = ((BasicBSONObject) ob).getObjectId("_id");
				if (null == oid) {
					// what's the use in throwing exception here? throw new DScabiException("_id is missing for file : " + fileName, "DBF.RAF.1");
					// let it continue to cleanup as much as possible
					continue;
				}
		    	Set<String> st = ob.keySet();
		    	if (st.containsAll(stMetaKeys)) {
		    		continue;
		    	} else {
					// remove file
					m_gridFSBucket.delete(oid);

		    	}
    		}
    	}
		return 0;
	}
	
	public String getLatestFileID(String fileName) throws DScabiException {
		
		// This call to removeFilesIncompleteMetaData() is needed because if the last file upload failed (network issue, etc.) 
		// that incomplete file entry will cause getLatestFileID() to throw exception. 
		// So good complete files already in DB will not be served.
		// The "" as file id below is just to enable method removeFilesIncompleteMetaData() to cleanup all incomplete files with this fileName
		// Don't call this as if a put is in progress for the same fileName, it will get deleted!!
		// // // removeFilesIncompleteMetaData(fileName, ""); 

		String latestFileID = null;
		long latestServerDateTime = 0;
		int n = 0;
		
		// take only those file entries for fileName with complete meta-data
        BasicDBObject documentQuery = new BasicDBObject();
    	documentQuery.put("PutFileName", fileName);
        documentQuery.append("PutStatus", "Completed");

	   	DBCursor cursorExist = m_table.find(documentQuery);
    	n = cursorExist.count();
    	if (1 == n) {
    		while (cursorExist.hasNext()) {
		    	DBObject ob = cursorExist.next();
		    	log.debug("handlePreviousVersions() result from ob {}", ob.toString());
		    	
		    	String fid = (String) ((BasicBSONObject) ob).getString("PutServerFileID");
				if (null == fid) {
					throw new DScabiException("PutServerFileID is missing for file : " + fileName, "DBF.GLF.1");
				}
				return fid;
    		}
    	
    	}  else if (0 == n) {
			log.debug("getLatestFileID() No matches for file : " + fileName + " with PutStatus=Completed");
			throw new DScabiException("getLatestFileID() No matches for file : " + fileName + " with PutStatus=Completed", "DBF.GLF.2");
    	} else {
    		while (cursorExist.hasNext()) {
		    	DBObject ob = cursorExist.next();
		    	log.debug("getLatestFileID() result from ob {}", ob.toString());
		    	
		    	// Analysis needed : can we just continue with next file entry instead of throwing exception?
		    	String fid = (String) ((BasicBSONObject) ob).getString("PutServerFileID");
				if (null == fid) {
					throw new DScabiException("PutServerFileID is missing for one version of file : " + fileName, "DBF.GLF.3");
				}
		    	String f = (String) ((BasicBSONObject) ob).getString("PutServerUploadDateTime");
				if (null == f) {
					throw new DScabiException("PutServerUploadDateTime is missing for one version of file : " + fileName + " file ID : " + fid, "DBF.GLF.4");
				}
				String f2 = (String) ((BasicBSONObject) ob).getString("PutLatestNumber");
				if (null == f2) {
					throw new DScabiException("PutLatestNumber is missing for one version of file : " + fileName + " file ID : " + fid, "DBF.GLF.5");
				}
				long lf2 = Long.parseLong(f);
				if (latestServerDateTime < lf2 && f2.equals("1")) {
					// proceed with other versions
					latestServerDateTime = lf2;
					latestFileID = fid;
				}
				
			}
    	}
    	return latestFileID;
	}
	
	public boolean isValidMetaData(String fileName, String strFileID) throws IOException, DScabiException {
		int n = 0;
		Set<String> stMetaKeys = new HashSet<String>();
		stMetaKeys.add("PutFileName");
		stMetaKeys.add("PutServerFileID");
		stMetaKeys.add("PutServerUploadDateTime");
		stMetaKeys.add("PutType");
		stMetaKeys.add("PutContentType");
		stMetaKeys.add("PutClientDateTime");
		stMetaKeys.add("PutClientDateTimeInMillis");
		stMetaKeys.add("PutClientDateTimeInNano");	
		stMetaKeys.add("PutStatus");
		stMetaKeys.add("PutLatestNumber");

        BasicDBObject documentQuery = new BasicDBObject();
		ObjectId fileID = new ObjectId(strFileID);
        // "_id" is MongoDB/GridFS specific meta data name inside fs.files collection for each file
        documentQuery.put("_id", fileID);
        
	   	DBCursor cursorExist = m_table.find(documentQuery);
    	n = cursorExist.count();
    	if (1 == n) {
			log.debug("isValidMetaData() Inside 1 == n");
    		while (cursorExist.hasNext()) {
		    	DBObject ob = cursorExist.next();
		    	log.debug("isValidMetaData() result from ob {}", ob.toString());
		    	Set<String> st = ob.keySet();
		    	if (st.containsAll(stMetaKeys)) {
		    		return true;
		    	} else {
		    		return false;
		    	}
    		}
    	}  else if (0 == n) {
			log.debug("isValidMetaData() No matches for file : " + fileName + " fileID : " + fileID.toHexString());
			throw new DScabiException("isValidMetaData() No matches for file : " + fileName + " fileID : " + fileID.toHexString(), "DBF.IVM.1");
			//return false;
    	} else {
			log.debug("isValidMetaData() Multiple matches for file : " + fileName + " fileID : " + fileID.toHexString());
			throw new DScabiException("isValidMetaData() Multiple matches for file : " + fileName + " fileID : " + fileID.toHexString(), "DBF.IVM.2");
			//return false;
    	}
		return false;
	}
	
	public long put(String fileName, String fullFilePath, String type, String contentType) throws IOException, DScabiException, ParseException {
		long time1;
		long time2;
		int n = 0;
		long total = 0;
		
        // Get the input stream
        time1 = System.currentTimeMillis();
        InputStream fromStream = new FileInputStream(fullFilePath);

        byte data[] = new byte[m_bufferSize];
        GridFSUploadStream uploadStream = m_gridFSBucket.openUploadStream(fileName, m_options);

        while ((n = fromStream.read(data)) > 0) {
            uploadStream.write(data, 0, n);
            total = total + n;
        }
        uploadStream.close();
        fromStream.close();
        
        updateMetaData(fileName, uploadStream.getFileId(), type, contentType);
        
        time2 = System.currentTimeMillis();
        
        log.debug("put() The fileId of the uploaded file is: " + uploadStream.getFileId().toHexString());
        log.debug("put() Upload time taken : time2 - time1 : " + (time2 - time1));
		
		return total;
	}

	public long put(String fileName, InputStream fromStream, String type, String contentType) throws IOException, DScabiException, ParseException {
		long time1;
		long time2;
		int n = 0;
		long total = 0;
        
        time1 = System.currentTimeMillis();

        byte data[] = new byte[m_bufferSize];
        GridFSUploadStream uploadStream = m_gridFSBucket.openUploadStream(fileName, m_options);

        while ((n = fromStream.read(data)) > 0) {
            uploadStream.write(data, 0, n);
            total = total + n;
        }
        uploadStream.close();
        fromStream.close();

        updateMetaData(fileName, uploadStream.getFileId(), type, contentType);

        time2 = System.currentTimeMillis();

        log.debug("put() The fileId of the uploaded file is: " + uploadStream.getFileId().toHexString());
        log.debug("put() Upload time taken : time2 - time1 : " + (time2 - time1));
		
		return total;
	}
	
	public long get(String fileName, String fullFilePath) throws IOException, DScabiException {
		long time1;
		long time2;
		int n = 0;
		long total = 0;
		
        time1 = System.currentTimeMillis();
        
        /* Reference : works using fileName and MongoDB specific .revision(-1) to get latest version of file
        GridFSDownloadByNameOptions downloadOptions = new GridFSDownloadByNameOptions().revision(-1); // latest file
        GridFSDownloadStream downloadStream = m_gridFSBucket.openDownloadStreamByName(fileName, downloadOptions);
        ObjectId fileID = downloadStream.getGridFSFile().getObjectId();
        log.debug("get() file id : " + downloadStream.getGridFSFile().getId().toString());
         */
        
        String fid = getLatestFileID(fileName);
        if (null == fid) {
        	throw new DScabiException("get() No file found with PutStatus=Completed for file : " + fileName, "DBF.GET.1");
        }
        log.debug("get() fid : {}", fid);
        GridFSDownloadStream downloadStream = m_gridFSBucket.openDownloadStream(new ObjectId(fid));
        log.debug("get() file id : " + downloadStream.getGridFSFile().getId().toString());
        
        /* This is only if using MongoDB specific .revision(-1) to get latest version of file
        if (false == isValidMetaData(fileName, fileID)) {
        	log.debug("get() latest file has invalid meta data"); 
        	downloadStream.close();
            downloadOptions.revision(-2); // prior version to latest file
            try {
            	downloadStream = m_gridFSBucket.openDownloadStreamByName(fileName, downloadOptions);
            }
            catch (Exception e) {
            	throw new DScabiException("get() No matches with valid meta data and PutStatus=Completed, No second latest also, for file : " + fileName, "DBF.GET.2");
            }
            
            ObjectId fileID2 = downloadStream.getGridFSFile().getObjectId();
            log.debug("get() file id : " + downloadStream.getGridFSFile().getId().toString());
            if (false == isValidMetaData(fileName, fileID2)) {
            	downloadStream.close();
            	throw new DScabiException("get() No matches with valid meta data and PutStatus=Completed for file : " + fileName, "DBF.GET.3");
            }
        }
		*/
        
        FileOutputStream toStream = new FileOutputStream(fullFilePath);

        //long fileLength = downloadStream.getGridFSFile().getLength();
        byte[] bytesToWriteTo = new byte[m_bufferSize];
        
        while ((n = downloadStream.read(bytesToWriteTo)) > 0) {
        	toStream.write(bytesToWriteTo, 0, n);
        	total = total + n;
        }
        downloadStream.close();
        toStream.close();
        
        time2 = System.currentTimeMillis();
        log.debug("get() Download time taken : time2 - time1 : " + (time2 - time1));
		
		return total;
	}

	public long get(String fileName, OutputStream toStream) throws IOException, DScabiException {
		long time1;
		long time2;
		int n = 0;
		long total = 0;
		
        time1 = System.currentTimeMillis();
        
        String fid = getLatestFileID(fileName);
        if (null == fid) {
        	throw new DScabiException("get() No file found with PutStatus=Completed for file : " + fileName, "DBF.GET2.1");
        }
        log.debug("get() fid : {}", fid);
        GridFSDownloadStream downloadStream = m_gridFSBucket.openDownloadStream(new ObjectId(fid));
        log.debug("get() file id : " + downloadStream.getGridFSFile().getId().toString());
        
        //long fileLength = downloadStream.getGridFSFile().getLength();
        byte[] bytesToWriteTo = new byte[m_bufferSize];
        
        while ((n = downloadStream.read(bytesToWriteTo)) > 0) {
        	toStream.write(bytesToWriteTo, 0, n);
        	total = total + n;
        }
        downloadStream.close();
        toStream.close();
        
        time2 = System.currentTimeMillis();
        log.debug("get() Download time taken : time2 - time1 : " + (time2 - time1));
		
		return total;
	}

	
}
