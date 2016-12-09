/**
 * @author Dilshad Mustafa
 * Copyright (c) Dilshad Mustafa
 * All Rights Reserved.
 * Created 21-Feb-2016
 * File Name : DMUtil.java
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

package com.dilmus.dilshad.scabi.common;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Dilshad Mustafa
 *
 */
public class DMUtil {

	private static final Logger log = LoggerFactory.getLogger(DMUtil.class);
	
	public static String toHexString(byte[] data){
	    String hex = "";
	    for(byte by : data) {
	        int b = by & 0xff;
	        if (Integer.toHexString(b).length() == 1)
	        	hex = hex + "0";
	        hex = hex + Integer.toHexString(b);
	    }
	    return hex;
	}
	
	public static byte[] toBytesFromHexStr(String hexStr) {
	    int len = hexStr.length();
	    byte[] data = new byte[len / 2];
	    for (int i = 0; i < len; i = i + 2) {
	        data[i / 2] = (byte) ((Character.digit(hexStr.charAt(i), 16) << 4) + Character.digit(hexStr.charAt(i+1), 16));
	    }
	    return data;
	}	

	public static byte[] toBytesFromInStream(InputStream in) throws IOException {
		ByteArrayOutputStream buffer = new ByteArrayOutputStream();
		int n = 0;
		byte[] data = new byte[64 * 1024 * 1024];
		while ((n = in.read(data, 0, data.length)) != -1) {
		  buffer.write(data, 0, n);
		}
		buffer.flush();

		data = null;
		System.gc();
		System.gc();
		
		byte b[] = buffer.toByteArray();
		buffer.close();
		return b;
	}

	public static byte[] toBytesFromInStreamForJavaFiles(InputStream in) throws IOException {
		ByteArrayOutputStream buffer = new ByteArrayOutputStream();
		int n = 0;
		byte[] data = new byte[1024 * 1024];
		while ((n = in.read(data, 0, data.length)) != -1) {
		  buffer.write(data, 0, n);
		}
		buffer.flush();

		data = null;
		System.gc();
		
		byte b[] = buffer.toByteArray(); 
		buffer.close();
		return b;
	}

	
	public static String preprocess(String s) {

		CharSequence cs1 = "@@";
		CharSequence cs2 = "@";
		String s1 = s.replace(cs1, "\\\"");
		String s2 = s1.replace(cs2, "\"");
		
		return s2;
		
	}
	
	public static String serverErrMsg(Throwable e) {
		
		String err = null;
		
		if (null == e)
			return "e is null";
		err = e.toString();
		if (null == e.getMessage())
			err = err + " Message is null ";
		else
			err = err + " Message : " + e.getMessage();
		if (null == e.getCause())
			err = err + " Cause is null ";
		else
			err = err + " Cause : " + e.getCause().toString();
		StackTraceElement stacka[] = e.getStackTrace();
		if (null == stacka)
			err = err + " StackTrace is null";
		else {
			err = err + " StackTrace :";
			for (StackTraceElement stack : stacka) {
				err = err + " " + stack.toString();
			}
		}
		
		return err;
	}
	
	public static String clientErrMsg(Throwable e) {
		
		String err = null;
		
		if (null == e)
			return "Client Side Exception/Error e is null";
		err = "Client Side Exception/Error " + e.toString();
		if (null == e.getMessage())
			err = err + " Message is null ";
		else
			err = err + " Message : " + e.getMessage();
		if (null == e.getCause())
			err = err + " Cause is null ";
		else
			err = err + " Cause : " + e.getCause().toString();
		StackTraceElement stacka[] = e.getStackTrace();
		if (null == stacka)
			err = err + " StackTrace is null";
		else {
			err = err + " StackTrace :";
			for (StackTraceElement stack : stacka) {
				err = err + " " + stack.toString();
			}
		}
		
		return err;
	}

	public static String errorMessage(Throwable e) {
		
		String err = null;
		
		if (null == e)
			return "e is null";
		err = e.toString();
		if (null == e.getMessage())
			err = err + " Message is null ";
		else
			err = err + " Message : " + e.getMessage();
		if (null == e.getCause())
			err = err + " Cause is null ";
		else
			err = err + " Cause : " + e.getCause().toString();
		StackTraceElement stacka[] = e.getStackTrace();
		if (null == stacka)
			err = err + " StackTrace is null";
		else {
			err = err + " StackTrace :";
			for (StackTraceElement stack : stacka) {
				err = err + " " + stack.toString();
			}
		}
		
		return err;
	}
	
	public static String getNamespaceStr(String resourceURLStr) throws DScabiException {
		if (false == resourceURLStr.startsWith("scabi:"))
			throw new DScabiException("resourceURLStr is not proper namespace URL string.", "UTL.GNS.4");

		int startNamespace = resourceURLStr.indexOf(':');
		if (startNamespace + 1 >= resourceURLStr.length())
			throw new DScabiException("resourceURLStr is not proper namespace URL string. Namespace is missing.", "UTL.GNS.1");
		int endNamespace = resourceURLStr.indexOf(':', startNamespace + 1);
		if (startNamespace + 1 == endNamespace)
			throw new DScabiException("resourceURLStr is not proper namespace URL string. Namespace is missing.", "UTL.GNS.2");

		String strNamespace = resourceURLStr.substring(startNamespace + 1, endNamespace);
		if (0 == strNamespace.length())
			throw new DScabiException("resourceURLStr is not proper namespace URL string. Namespace is missing.", "UTL.GNS.3");

		return strNamespace;
		
	}
	
	public static String getResourceName(String resourceURLStr) throws DScabiException {
		if (false == resourceURLStr.startsWith("scabi:"))
			throw new DScabiException("resourceURLStr is not proper namespace URL string.", "UTL.GRN.5");

		int startNamespace = resourceURLStr.indexOf(':');
		if (startNamespace + 1 >= resourceURLStr.length())
			throw new DScabiException("resourceURLStr is not proper namespace URL string. Namespace is missing.", "UTL.GRN.1");
		int endNamespace = resourceURLStr.indexOf(':', startNamespace + 1);
		if (startNamespace + 1 == endNamespace)
			throw new DScabiException("resourceURLStr is not proper namespace URL string. Namespace is missing.", "UTL.GRN.2");
		if (endNamespace + 1 >= resourceURLStr.length())
			throw new DScabiException("resourceURLStr is not proper namespace URL string. Resource name is missing.", "UTL.GRN.3");
		
		String strResourceName = resourceURLStr.substring(endNamespace + 1, resourceURLStr.length());
		strResourceName.replace(" ", "");
		
		if (0 == strResourceName.length() || strResourceName.contains(" "))
			throw new DScabiException("resourceURLStr is not proper namespace URL string. Resource name is missing.", "UTL.GRN.4");

		return strResourceName;
		
	}

	public static boolean isNamespaceURLStr(String resourceURLStr) {
		boolean check = true;
		
		if (false == resourceURLStr.startsWith("scabi:"))
			check = false; //throw new DScabiException("resourceURLStr is not proper namespace URL string.", "UTL.GRN.5");

		int startNamespace = resourceURLStr.indexOf(':');
		if (startNamespace + 1 >= resourceURLStr.length())
			check = false; //throw new DScabiException("resourceURLStr is not proper namespace URL string. Namespace is missing.", "UTL.GRN.1");
		int endNamespace = resourceURLStr.indexOf(':', startNamespace + 1);
		if (startNamespace + 1 == endNamespace)
			check = false; //throw new DScabiException("resourceURLStr is not proper namespace URL string. Namespace is missing.", "UTL.GRN.2");
		if (endNamespace + 1 >= resourceURLStr.length())
			check = false; //throw new DScabiException("resourceURLStr is not proper namespace URL string. Resource name is missing.", "UTL.GRN.3");
		
		String strResourceName = resourceURLStr.substring(endNamespace + 1, resourceURLStr.length());
		strResourceName.replace(" ", "");
		
		if (0 == strResourceName.length() || strResourceName.contains(" "))
			check = false; //throw new DScabiException("resourceURLStr is not proper namespace URL string. Resource name is missing.", "UTL.GRN.4");
	
		return check;
	
	}
	
	/*
	public static int listSize(List<?> list) {
		ListIterator<?> itr = list.listIterator();
		int size = 0;
		
		while (itr.hasNext()) {
			itr.next();
			size++;
		}
		return size;
	}
	
	public static <T> T listGet(List<T> list, int index) throws DScabiException {
		ListIterator<T> itr = list.listIterator();
		int i = -1;

		if (index < 0)
			throw new DScabiException("listGet() index < 0", "DMU.LGT.1");
	
		while (itr.hasNext()) {
			itr.next();
			i++;

			if (i == index) {
				itr.previous();
				return itr.next();
			}
		}
		
		//if (index > i)
		//	throw new DScabiException("listGet() index > size of list", "DMU.LGT.2");
		
		throw new DScabiException("listGet() index > size of list", "DMU.LGT.2");
			
	}
	*/
	
	public static <T> ListIterator<T> iteratorBefore(List<T> list, long index) throws DScabiException {
		ListIterator<T> itr = list.listIterator();
		long i = -1;

		if (index < 0)
			throw new DScabiException("iteratorBefore() index < 0", "DMU.IBE.1");
	
		while (itr.hasNext()) {
			itr.next();
			i++;

			if (i == index) {
				itr.previous();
				return itr;
			}
		}
		
		//if (index > i)
		//	throw new DScabiException("iteratorBefore() index > size of list", "DMU.IBE.2");
		
		throw new DScabiException("iteratorBefore() index > size of list", "DMU.IBE.2");
			
	}

	public static long hashString(String s) {
		byte[] bytea = s.getBytes();
		int byteaLen = bytea.length;
		//System.out.println("hashString() byteaLen : " + byteaLen);
		if (0 == byteaLen)
			return 0;
		
		int j = 0;
		byte b = 0;
		long hash = 0;
		
		for (int i = 0; i < Long.BYTES; i++) {
		
			//System.out.println("hashString() i : " + i);
			//System.out.println("hashString() j : " + j);
			if (j >= byteaLen)
				return hash;
			//System.out.println("hashString() bytea[j] : " + bytea[j]);
			if (0 == bytea[j])
				j++;
			b = bytea[j];

			if (0 == hash)
				hash = b;
			else 
				hash = (hash << 8) + b;
			
			j++;
		}
		
		return hash;
	}
	
	public static int deleteFileDir(String dirPath) {
		
		File f = new File(dirPath);
		
		if (f.exists() == false)
			return 0;
		if (f.isDirectory()) {
			File[] fa = f.listFiles();
			for (File e : fa) {
				if (e.isDirectory())
					deleteFileDir(e);
				else if (e.isFile()) {
					// System.out.println("deleteFileDir(str) Deleting file : " + e);
					log.debug("deleteFileDir(str) Deleting file : {}", e);
					e.delete();
				}
			}	
			// System.out.println("deleteFileDir(str) Deleting dir : " + f);
			log.debug("deleteFileDir(str) Deleting dir : {}", f);
			f.delete();
		}	
		else if (f.isFile()){
			// System.out.println("deleteFileDir(str) Deleting2 file : " + f);
			log.debug("deleteFileDir(str) Deleting2 file : {}", f);
			f.delete();
		}
		return 0;
	}
		
	public static int deleteFileDir(File f) {
		if (f.exists() == false)
			return 0;
		if (f.isDirectory()) {
			File[] fa = f.listFiles();
			for (File e : fa) {
				if (e.isDirectory())
					deleteFileDir(e);
				else if (e.isFile()) {
					// System.out.println("deleteFileDir(f) Deleting file : " + e);
					log.debug("deleteFileDir(f) Deleting file : {}", e);
					e.delete();
				}
			}
			// System.out.println("deleteFileDir(f) Deleting dir : " + f);
			log.debug("deleteFileDir(f) Deleting dir : {}", f);
			f.delete();
		}	
		else if (f.isFile()){
			// System.out.println("deleteFileDir(f) Deleting2 file : " + f);
			log.debug("deleteFileDir(f) Deleting2 file : {}", f);
			f.delete();
		}
		
		return 0;
	}	
	
	public static String getUserDir() {
		// pass specific path through Java VM argument as -Duser.dir="/home/anees/testdata/bigfile/tutorial/"
		// if no value is set in user.dir property by User, then user.dir property gives current working directory from where java command is run
		String dirPath = System.getProperty("user.dir");
		// System.out.println("getUserDir() user.dir : " + dirPath);
		log.debug("user.dir : {}", dirPath);
		return dirPath;
	}
	
	public static String getWorkingDir() {
		File f = new File(".");
		String fullPath = f.getAbsolutePath();
		// System.out.println("getWorkingDir() fullPath : " + fullPath);
		log.debug("fullPath : {}", fullPath);
		String fPath = fullPath.substring(0, fullPath.length() - 1);
		if (fPath.charAt(fPath.length() - 1) == File.separatorChar && fPath.charAt(fPath.length() - 2) == File.separatorChar)
			fPath = fPath.substring(0, fPath.length() - 1);
        // System.out.println("getWorkingDir() Using File approach : " + fPath);
        log.debug("Using File approach : {}", fPath);
        return fPath;
	}	
	
	public static String appendToDirPath(String dirPath, String fileOrDirPath) {

		String path = null;
		
		if (dirPath.charAt(dirPath.length() - 1) == File.separatorChar)
			path = dirPath + fileOrDirPath;
		else
			path = dirPath + File.separator + fileOrDirPath;
		// System.out.println("appendToDirPath() path : " + path);
		log.debug("path : {}", path);
		return path;
	}
	
	public static int createDirIfAbsent(String dirPath) throws DScabiException {
		File f = new File(dirPath);
		if (f.exists())
			return 0;
		if (f.mkdir())
			return 0;
		else 
			throw new DScabiException("Failed to create directory : " + dirPath, "UTL.CDI.1");
	}
	
	public static int createDirsIfAbsent(String dirPath) throws DScabiException {
		File f = new File(dirPath);
		if (f.exists())
			return 0;
		if (f.mkdirs())
			return 0;
		else 
			throw new DScabiException("Failed to create directories : " + dirPath, "UTL.CDSI.1");
	}
	
}
