/**
 * @author Dilshad Mustafa
 * Copyright (c) Dilshad Mustafa
 * All Rights Reserved.
 * @since 29-Feb-2016
 * File Name : DComputeAsyncConfig.java
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

package com.dilmus.dilshad.scabi.core.async;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dilmus.dilshad.scabi.common.DMClassLoader;
import com.dilmus.dilshad.scabi.common.DMJson;
import com.dilmus.dilshad.scabi.common.DScabiException;
import com.dilmus.dilshad.scabi.core.DComputeUnit;

/**
 * @author Dilshad Mustafa
 *
 */
public class DComputeAsyncConfig {
	public final static int CODE = 1;
	public final static int CLASS = 2;
	public final static int OBJECT = 3;
	public final static int CLASSNAMEINJAR = 4;
	
	private final Logger log = LoggerFactory.getLogger(DComputeAsyncConfig.class);
	private DComputeUnit m_unit = null;
	private Class<? extends DComputeUnit> m_class = null;
	private String m_code = null;
	private String m_jarFilePath = null;
	private String m_classNameInJar = null;
	
	private int m_configType = 0;
	private String m_jsonStrInput = null;
	private HashMap<String, String> m_outputMap = null;
	private int m_maxSplit = 1;
	private int m_maxRetry = 0;
	
	private boolean m_isSplitSet = false;
	private int m_startSplit = -1;
	private int m_endSplit = -1;
	
	private boolean m_isJarFilePathListSet = false;
	private List<String> m_jarFilePathList = null;

	private boolean m_isComputeUnitJarsSet = false;
	private DMClassLoader m_dcl = null;
	
	public int setComputeUnitJars(DMClassLoader dcl) {
		m_isComputeUnitJarsSet = true;
		m_dcl = dcl;
		return 0;
	}

	public boolean isComputeUnitJarsSet() {
		return m_isComputeUnitJarsSet;
	}

	public DMClassLoader getComputeUnitJars() {
		return m_dcl;
	}
		
	public DComputeAsyncConfig(DComputeUnit unit) {
		m_unit = unit;
		m_configType = DComputeAsyncConfig.OBJECT;
		m_maxSplit = 1;
		
		m_jarFilePathList = new ArrayList<String>();
		m_jsonStrInput = DMJson.empty();
	}
	
	public DComputeAsyncConfig(Class<? extends DComputeUnit> cls) {
		m_class = cls;
		m_configType = DComputeAsyncConfig.CLASS;
		m_maxSplit = 1;
		
		m_jarFilePathList = new ArrayList<String>();
		m_jsonStrInput = DMJson.empty();
	}

	public DComputeAsyncConfig(String code) {
		m_code = code;
		m_configType = DComputeAsyncConfig.CODE;
		m_maxSplit = 1;
		
		m_jarFilePathList = new ArrayList<String>();
		m_jsonStrInput = DMJson.empty();
	}

	public DComputeAsyncConfig(String jarFilePath, String classNameInJar) {
		m_jarFilePath = jarFilePath;
		m_classNameInJar = classNameInJar;
		m_configType = DComputeAsyncConfig.CLASSNAMEINJAR;
		m_maxSplit = 1;
		
		m_jarFilePathList = new ArrayList<String>();
		m_jsonStrInput = DMJson.empty();
	}

	public boolean isJarFilePathListSet() {
		return m_isJarFilePathListSet;
	}
	
	public int setJarFilePathFromList(List<String> jarFilePathList) {
		m_jarFilePathList.addAll(jarFilePathList);
		m_isJarFilePathListSet = true;
		return 0;
	}
	
	public List<String> getJarFilePathList() {
		return m_jarFilePathList;
	}
	
	int setSplitRange(int startSplit, int endSplit) throws DScabiException {
		
		if (startSplit <= 0) {
			throw new DScabiException("startSplit should not be <= 0", "CAC.SSR.1");
		}
		if (endSplit <= 0) {
			throw new DScabiException("endSplit should not be <= 0", "CAC.SSR.2");
		}
		if (startSplit > endSplit) {
			throw new DScabiException("startSplit should not be > endSplit", "CAC.SSR.3");
		}
		m_isSplitSet = true;
		m_startSplit = startSplit;
		m_endSplit = endSplit;

		return 0;
	}
	
	public boolean isSplitSet() {
		return m_isSplitSet;
	}
	
	public int getStartSplit() {
		return m_startSplit;
	}
	
	public int getEndSplit() {
		return m_endSplit;
	}

	public int getConfigType() {
		return m_configType;
	}
	
	public DComputeUnit getComputeUnit() {
		return m_unit;
	}

	public Class<? extends DComputeUnit> getComputeClass() {
		return m_class;
	}
	
	public String getComputeCode() {
		return m_code;
	}

	public String getJarFilePath() {
		return m_jarFilePath;
	}
	
	public String getClassNameInJar() {
		return m_classNameInJar;
	}
	
	public int setInput(String jsonInput) {
		m_jsonStrInput = jsonInput;
		return 0;
	}
	
	public int setOutput(HashMap<String, String> outputMap) {
		m_outputMap = outputMap;
		return 0;
	}
	
	public int setResult(int splitno, String result) {
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

	public int appendResult(int splitno, String result) {
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

	
	public boolean isResultSet(int splitno) {
		if (m_outputMap != null) {
			if (m_outputMap.containsKey("" + splitno))
				return true;
		}
		return false;
	}
	
	public int setMaxSplit(int maxSplit) {
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
	
	public int getMaxSplit() {
		return m_maxSplit;
	}
	
	public int getMaxRetry() {
		return m_maxRetry;
	}

}
