/**
 * @author Dilshad Mustafa
 * Copyright (c) Dilshad Mustafa
 * All Rights Reserved.
 * Created 18-Jul-2016
 * File Name : DataUnitConfig.java
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

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dilmus.dilshad.scabi.common.DMClassLoader;
import com.dilmus.dilshad.scabi.common.DMCounter;
import com.dilmus.dilshad.scabi.common.DMJson;
import com.dilmus.dilshad.scabi.common.DMUtil;
import com.dilmus.dilshad.scabi.common.DScabiException;
import com.dilmus.dilshad.scabi.core.DComputeUnit;
import com.dilmus.dilshad.scabi.core.DataUnit;

/**
 * @author Dilshad Mustafa
 *
 */
public class DataUnitConfig implements IConfig {
	
	public final static int CFG_TYPE_CLASS = 1;
	public final static int CFG_TYPE_CLASS_OF_INTERFACE = 2;
	public final static int CFG_TYPE_OBJECT = 3;
	public final static int CFG_TYPE_OBJECT_OF_INTERFACE = 4;
	
	private final Logger log = LoggerFactory.getLogger(DataUnitConfig.class);
	private DataUnit m_dataUnitObj = null;
	private Class<?> m_dataUnitClass = null;
	// Not used private String m_code = null;
	// Not used private String m_jarFilePath = null;
	// Not used private String m_classNameInJar = null;
	
	private int m_configType = 0;
	private String m_jsonStrInput = null;
	private HashMap<String, String> m_outputMap = null;
	private long m_maxSplit = 1;
	private int m_maxRetry = 0;
	
	private boolean m_isJarFilePathListSet = false;
	private LinkedList<String> m_jarFilePathList = null;

	private boolean m_isComputeUnitJarsSet = false;
	private DMClassLoader m_dcl = null;
	
	private String m_jobId = null;
	private String m_configId = null;
	
	private static final DMCounter M_DMCOUNTER = new DMCounter();
	
	private HashMap<String, String> m_statusMap = null;
	
	private String m_dataId = null;
	
	// for java class, object .class file
	private String m_javaFileHexStr = null;
	// for all addJar()'s jar files
	private HashMap<String, String> m_mapJarFilePathAsIdHexStr = new HashMap<String, String>();
	private String m_allJarFilesHexStrJsonStr = null;
	private DMJson m_allJarFilesHexStrDMJson = null;
	// for class name in jar's jar file
	// Not used private String m_forClassNameInJarFileHexStr = null;
	// for Compute Unit jar files
	private String m_allComputeUnitJarFilesHexStrJsonStr = null;
	private DMJson m_allComputeUnitJarFilesHexStrDMJson = null;
	// for combined jars of .addJar() and .addComputeUnitJars()
	private boolean m_isCombinedJarsSet = false;
	private String m_combinedJarFilesHexStrJsonStr = null;
	private DMJson m_combinedJarFilesHexStrDMJson = null;
	
	private int loadJavaFileAsHexStr(Class<?> cls) throws IOException {

		String className = cls.getName();
    	log.debug("loadJavaFileAsHexStr() className  : {}", className);
  		String classAsPath = className.replace('.', '/') + ".class";
    	log.debug("loadJavaFileAsHexStr() classAsPath  : {}", classAsPath);
		InputStream in = cls.getClassLoader().getResourceAsStream(classAsPath);
  		byte b[] = DMUtil.toBytesFromInStreamForJavaFiles(in);
  		in.close();
  		//log.debug("loadJavaFileAsHexStr() b[] as string : {}", b.toString());
  		String hexStr = DMUtil.toHexString(b);
  		//log.debug("loadJavaFileAsHexStr() Hex string is : {}", hexStr);
  		m_javaFileHexStr = hexStr;
  		
		return 0;
	}
	
	private int loadJarsAsHexStr() throws IOException {
		DMJson dmjsonout = null;
		
		if (0 == m_jarFilePathList.size()) {
			m_allJarFilesHexStrJsonStr = null;
			m_allJarFilesHexStrDMJson = null;
			log.debug("loadJarsAsHexStr() Information only : m_jarFilePathList is empty");
			return 0;
		}
		
		for (String jarFilePath : m_jarFilePathList) {
			log.debug("loadJarsAsHexStr() jarFilePath : {}", jarFilePath);
	  		InputStream in = new FileInputStream(jarFilePath);
	  		byte b[] = DMUtil.toBytesFromInStreamForJavaFiles(in);
	  		in.close();
	  		//log.debug("loadJarsAsHexStr() b[] as string : {}", b.toString());
	  		String hexStr = DMUtil.toHexString(b);
	  		//log.debug("loadJarsAsHexStr() Hex string is : {}", hexStr);
	  		m_mapJarFilePathAsIdHexStr.put(jarFilePath, hexStr);
	  		
	  		if (null == dmjsonout) {
	  			dmjsonout = new DMJson(jarFilePath, hexStr);
	  		} else {
	  			dmjsonout = dmjsonout.add(jarFilePath, hexStr);
	  		}
		}
		m_allJarFilesHexStrJsonStr = dmjsonout.toString();
		//log.debug("loadJarsAsHexStr() m_allJarFilesHexStrJsonStr : {}", m_allJarFilesHexStrJsonStr);
		m_allJarFilesHexStrDMJson = dmjsonout;
		
		return 0;
	}
	
	/* Not used
	private int loadForClassNameInJarAsHexStr(String forClassNameInJarFilePath) throws IOException {
  		InputStream in = new FileInputStream(forClassNameInJarFilePath);
  		byte b[] = DMUtil.toBytesFromInStreamForJavaFiles(in);
  		in.close();
  		//log.debug("loadForClassNameInJarAsHexStr() b[] as string : {}", b.toString());
  		String hexStr = DMUtil.toHexString(b);
  		//log.debug("loadForClassNameInJarAsHexStr() Hex string is : {}", hexStr);
  		m_forClassNameInJarFileHexStr = hexStr;
  		
		return 0;
	}
	*/
	
	private int loadComputeUnitJars() throws IOException {
		
		DMJson dmjsonout = null;
	
		HashMap<String, byte[]> map = m_dcl.getMapJarFilePathJarBytes();
		
		if (0 == map.size()) {
			log.debug("loadComputeUnitJars() Information only : m_dcl.getMapJarFilePathJarBytes() is empty");
			m_allComputeUnitJarFilesHexStrJsonStr = null;
			m_allComputeUnitJarFilesHexStrDMJson = null;
			return 0;
		}
		
		Set<String> st = map.keySet();
		for (String jarFilePath : st) {
			log.debug("loadComputeUnitJars() jarFilePath : {}", jarFilePath);
	  		
	  		byte b[] = map.get(jarFilePath);
	  		
	  		//log.debug("loadComputeUnitJars() b[] as string : {}", b.toString());
	  		String hexStr = DMUtil.toHexString(b);
	  		//log.debug("loadComputeUnitJars() Hex string is : {}", hexStr);
	  		if (null == dmjsonout) {
	  			dmjsonout = new DMJson(jarFilePath, hexStr);
	  		} else {
	  			dmjsonout = dmjsonout.add(jarFilePath, hexStr);
	  		}
	  	}
		m_allComputeUnitJarFilesHexStrJsonStr = dmjsonout.toString();
		//log.debug("loadComputeUnitJars() m_allComputeUnitJarFilesHexStrJsonStr : {}", m_allComputeUnitJarFilesHexStrJsonStr);
		m_allComputeUnitJarFilesHexStrDMJson = dmjsonout;
		
		return 0;
	}
	
	public int combineJarsForAddJarJsonField() throws IOException {
		// Don't modify this object
		DMJson dmjsonout = m_allComputeUnitJarFilesHexStrDMJson;
		DMJson outJson = null;
		
		if (m_isCombinedJarsSet)
			return 0;
		
		if (dmjsonout != null) {
			
			if (null == m_allJarFilesHexStrDMJson) {
				// Don't modify this object
				outJson = m_allComputeUnitJarFilesHexStrDMJson;
				m_combinedJarFilesHexStrJsonStr = m_allComputeUnitJarFilesHexStrJsonStr;
				m_combinedJarFilesHexStrDMJson = m_allComputeUnitJarFilesHexStrDMJson;
				log.debug("combineJarsForAddJarJsonField() - 1 - m_combinedJarFilesHexStrJsonStr : {}", m_combinedJarFilesHexStrJsonStr);
			} else {
				// Create new, don't modify original DMJson object
				DMJson dmjsonnew = new DMJson(m_allJarFilesHexStrJsonStr);
				Set<String> st2 = dmjsonout.keySet();
				for (String s2 : st2) {
					dmjsonnew = dmjsonnew.add(s2, dmjsonout.getString(s2));
				}
				outJson = dmjsonnew;
				m_combinedJarFilesHexStrJsonStr = dmjsonnew.toString();
				m_combinedJarFilesHexStrDMJson = dmjsonnew;
				log.debug("combineJarsForAddJarJsonField() - 2 - m_combinedJarFilesHexStrJsonStr : {}", m_combinedJarFilesHexStrJsonStr);
			}
		} else {
			if (null == m_allJarFilesHexStrDMJson) {
				outJson = null;
				m_combinedJarFilesHexStrJsonStr = null;
				m_combinedJarFilesHexStrDMJson = null;
				log.debug("combineJarsForAddJarJsonField() - 3 - Information only : m_combinedJarFilesHexStrJsonStr is null");
			} else {
				// Don't modify this object
				outJson = m_allJarFilesHexStrDMJson;
				m_combinedJarFilesHexStrJsonStr = m_allJarFilesHexStrJsonStr;
				m_combinedJarFilesHexStrDMJson = m_allJarFilesHexStrDMJson;
				log.debug("combineJarsForAddJarJsonField() - 4 - m_combinedJarFilesHexStrJsonStr : {}", m_combinedJarFilesHexStrJsonStr);
			}
		}
		m_isCombinedJarsSet = true;
		return 0;		
	}
	
	public String getJavaFileAsHexStr() {
		return m_javaFileHexStr;
	}
	
	public String getJarAsHexStr(String jarFilePathAsId) throws DScabiException {
		if (m_mapJarFilePathAsIdHexStr.containsKey(jarFilePathAsId))
			return m_mapJarFilePathAsIdHexStr.get(jarFilePathAsId);
		else
			throw new DScabiException("jarFilePathAsId is not found in map m_mapJarFilePathAsIdHexStr. jarFilePathAsId : " + jarFilePathAsId, "Driver.CAC.GJR.1");
	}
	
	public String getAllJarFilesHexStrJsonStr() {
		return m_allJarFilesHexStrJsonStr;
	}
	
	public DMJson getAllJarFilesHexStrDMJson() {
		return m_allJarFilesHexStrDMJson;
	}
	
	/* Not used
	public String getForClassNameInJarAsHexStr() {
		return m_forClassNameInJarFileHexStr;
	}
	*/
	
	public String getAllComputeUnitJarFilesHexStrJsonStr() {
		return m_allComputeUnitJarFilesHexStrJsonStr;
	}
	
	public DMJson getAllComputeUnitJarFilesHexStrDMJson() {
		return m_allComputeUnitJarFilesHexStrDMJson;
	}
	
	public String getCombinedJarFilesHexStrJsonStr() {
		return m_combinedJarFilesHexStrJsonStr;
	}
	
	public DMJson getCombinedJarFilesHexStrDMJson() {
		return m_combinedJarFilesHexStrDMJson;
	}
	public boolean isCombinedJarsSet() {
		return m_isCombinedJarsSet;
	}
	
	public int setDataId(String dataId) {
		m_dataId = dataId;
		return 0;
	}
	
	public String getDataId() {
		return m_dataId;
	}
	
	public int setJobId(String jobId) {
		m_jobId = jobId;
		m_configId = jobId + "_" + M_DMCOUNTER.inc();
		return 0;
	}
	
	public String getJobId() {
		return m_jobId;
	}

	public String getConfigId() {
		return m_configId;
	}

	public int setComputeUnitJars(DMClassLoader dcl) throws IOException {
		m_isComputeUnitJarsSet = true;
		m_dcl = dcl;
		loadComputeUnitJars();
		return 0;
	}

	public boolean isComputeUnitJarsSet() {
		return m_isComputeUnitJarsSet;
	}

	public DMClassLoader getComputeUnitJars() {
		return m_dcl;
	}
		
	public DataUnitConfig(DataUnit unit) throws IOException {
		m_dataUnitObj = unit;
		m_configType = DataUnitConfig.CFG_TYPE_OBJECT;
		m_maxSplit = 1;
		
		m_jarFilePathList = new LinkedList<String>();
		m_jsonStrInput = DMJson.empty();
		
		m_jobId = DMJson.empty();
		
		// Previous works m_configId = UUID.randomUUID().toString() + "_" + System.nanoTime() + "_" + M_DMCOUNTER.inc();
		// Previous works m_configId = m_configId.replace('-', '_');
		
		m_configId = DMJson.empty();
		
		m_statusMap = new HashMap<String, String>();
		
		loadJavaFileAsHexStr(unit.getClass());
	}
	
	public DataUnitConfig(Class<?> cls) throws IOException {
		m_dataUnitClass = cls;
		m_configType = DataUnitConfig.CFG_TYPE_CLASS;
		m_maxSplit = 1;
		
		m_jarFilePathList = new LinkedList<String>();
		m_jsonStrInput = DMJson.empty();
		
		m_jobId = DMJson.empty();
		
		// Previous works m_configId = UUID.randomUUID().toString() + "_" + System.nanoTime() + "_" + M_DMCOUNTER.inc();
		// Previous works m_configId = m_configId.replace('-', '_');
		
		m_configId = DMJson.empty();
		
		m_statusMap = new HashMap<String, String>();
		
		loadJavaFileAsHexStr(cls);
	}

	public boolean isJarFilePathListSet() {
		return m_isJarFilePathListSet;
	}
	
	public int setJarFilePathFromList(List<String> jarFilePathList) throws IOException {
		m_jarFilePathList.addAll(jarFilePathList);
		m_isJarFilePathListSet = true;
		
		loadJarsAsHexStr();
		
		return 0;
	}
	
	public List<String> getJarFilePathList() {
		return m_jarFilePathList;
	}
	
	public int getConfigType() {
		return m_configType;
	}
	
	public DataUnit getDataUnitObject() {
		return m_dataUnitObj;
	}

	public Class<?> getDataUnitClass() {
		return m_dataUnitClass;
	}
	
	public int setInput(String jsonInput) {
		m_jsonStrInput = jsonInput;
		return 0;
	}
	
	public int setOutput(HashMap<String, String> outputMap) {
		m_outputMap = outputMap;
		return 0;
	}
	
	public int setResult(long splitno, String result) {
		if (m_outputMap != null) {
			m_outputMap.put("" + splitno, result);
			log.debug("setResult() Setting result for splitno : {}, {}", splitno, result);
		}
		
		/* Debugging
    	Set<String> st = m_outputMap.keySet();
    	for (String s : st) {
    		log.debug("setResult() for {} : {}", s, m_outputMap.get(s));
    	}
		*/
		return 0;
	}

	public int setStatus(long splitno, String result) {
		if (m_statusMap != null) {
			m_statusMap.put("" + splitno, result);
			log.debug("setStatus() Setting result for splitno : {}, {}", splitno, result);
		}
		
		/* Debugging
    	Set<String> st = m_statusMap.keySet();
    	for (String s : st) {
    		log.debug("setStatus() for {} : {}", s, m_statusMap.get(s));
    	}
		*/
		return 0;
	}

	
	public int appendResult(long splitno, String result) {
		if (m_outputMap != null) {
			if (m_outputMap.containsKey("" + splitno)) {
				String str = m_outputMap.get("" + splitno);
				str = str + " Appended Result : " + result;
				m_outputMap.put("" + splitno, str);
			} else {
				m_outputMap.put("" + splitno, result);
			}
			log.debug("appendResult() Setting result for splitno : {}, {}", splitno, result);
		}
		
		/* Debugging
    	Set<String> st = m_outputMap.keySet();
    	for (String s : st) {
    		log.debug("appendResult() for {} : {}", s, m_outputMap.get(s));
    	}
		*/
		return 0;
	}

	
	public boolean isResultSet(long splitno) {
		if (m_outputMap != null) {
			if (m_outputMap.containsKey("" + splitno))
				return true;
		}
		return false;
	}
	
	public boolean isStatusSet(long splitno) {
		if (m_statusMap != null) {
			if (m_statusMap.containsKey("" + splitno))
				return true;
		}
		return false;
	}
	
	public int setMaxSplit(long maxSplit) {
		m_maxSplit = maxSplit;
		return 0;
	}
	
	public int setMaxRetry(int maxRetry) {
		m_maxRetry = maxRetry;
		return 0;
	}
	
	public String getInput() {
		return m_jsonStrInput;
	}
	
	public HashMap<String, String> getOutput() {
		return m_outputMap;
	}
	
	public long getMaxSplit() {
		return m_maxSplit;
	}
	
	public int getMaxRetry() {
		return m_maxRetry;
	}

}
