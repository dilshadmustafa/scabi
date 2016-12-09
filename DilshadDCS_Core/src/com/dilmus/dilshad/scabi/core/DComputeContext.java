/**
 * @author Dilshad Mustafa
 * Copyright (c) Dilshad Mustafa
 * All Rights Reserved.
 * Created 07-Feb-2016
 * File Name : DComputeContext.java
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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Set;
import java.util.Map.Entry;

/* Previous works
import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonNumber;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonReader;
import javax.json.JsonValue;
*/

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * @author Dilshad Mustafa
 *
 */
public class DComputeContext {

	private static final Logger log = LoggerFactory.getLogger(DComputeContext.class);
	private static final HashMap<String, ObjectMapper> m_mapThreadIdOM = new HashMap<String, ObjectMapper>();
	private ObjectMapper m_objectMapper = null;
	private ObjectNode m_root = null;
	private boolean m_isChanged = true;
	
	private String m_jsonString = null;
	private boolean m_isDsonEmpty = false;
	
	public DComputeContext(String jsonString) throws IOException {
		m_jsonString = jsonString;
		
		long thisThreadId = Thread.currentThread().getId();
		synchronized(m_mapThreadIdOM) {
			if (m_mapThreadIdOM.containsKey("" + thisThreadId))
				m_objectMapper = m_mapThreadIdOM.get("" + thisThreadId);
			else {
				m_objectMapper = new ObjectMapper();
				m_mapThreadIdOM.put("" + thisThreadId, m_objectMapper);
			}
		}
		m_root = (ObjectNode) m_objectMapper.readTree(jsonString);
	}
	
	public DComputeContext() {
		
		long thisThreadId = Thread.currentThread().getId();
		synchronized(m_mapThreadIdOM) {
			if (m_mapThreadIdOM.containsKey("" + thisThreadId))
				m_objectMapper = m_mapThreadIdOM.get("" + thisThreadId);
			else {
				m_objectMapper = new ObjectMapper();
				m_mapThreadIdOM.put("" + thisThreadId, m_objectMapper);
			}
		}

		m_root = m_objectMapper.createObjectNode();
		m_isDsonEmpty = true;
	}

	public DComputeContext(String key, String value) {
		long thisThreadId = Thread.currentThread().getId();
		synchronized(m_mapThreadIdOM) {
			if (m_mapThreadIdOM.containsKey("" + thisThreadId))
				m_objectMapper = m_mapThreadIdOM.get("" + thisThreadId);
			else {
				m_objectMapper = new ObjectMapper();
				m_mapThreadIdOM.put("" + thisThreadId, m_objectMapper);
			}
		}

		m_root = m_objectMapper.createObjectNode();
		m_root.put(key, value);
	}
	
	public DComputeContext(String key, int value) {
		long thisThreadId = Thread.currentThread().getId();
		synchronized(m_mapThreadIdOM) {
			if (m_mapThreadIdOM.containsKey("" + thisThreadId))
				m_objectMapper = m_mapThreadIdOM.get("" + thisThreadId);
			else {
				m_objectMapper = new ObjectMapper();
				m_mapThreadIdOM.put("" + thisThreadId, m_objectMapper);
			}
		}

		m_root = m_objectMapper.createObjectNode();
		m_root.put(key, value);
	}
	
	public DComputeContext(String key, long value) {
		long thisThreadId = Thread.currentThread().getId();
		synchronized(m_mapThreadIdOM) {
			if (m_mapThreadIdOM.containsKey("" + thisThreadId))
				m_objectMapper = m_mapThreadIdOM.get("" + thisThreadId);
			else {
				m_objectMapper = new ObjectMapper();
				m_mapThreadIdOM.put("" + thisThreadId, m_objectMapper);
			}
		}

		m_root = m_objectMapper.createObjectNode();
		m_root.put(key, value);
	}
	
	public DComputeContext(String key, float value) {
		long thisThreadId = Thread.currentThread().getId();
		synchronized(m_mapThreadIdOM) {
			if (m_mapThreadIdOM.containsKey("" + thisThreadId))
				m_objectMapper = m_mapThreadIdOM.get("" + thisThreadId);
			else {
				m_objectMapper = new ObjectMapper();
				m_mapThreadIdOM.put("" + thisThreadId, m_objectMapper);
			}
		}

		m_root = m_objectMapper.createObjectNode();
		m_root.put(key, value);
	}

	public DComputeContext(String key, double value) {
		long thisThreadId = Thread.currentThread().getId();
		synchronized(m_mapThreadIdOM) {
			if (m_mapThreadIdOM.containsKey("" + thisThreadId))
				m_objectMapper = m_mapThreadIdOM.get("" + thisThreadId);
			else {
				m_objectMapper = new ObjectMapper();
				m_mapThreadIdOM.put("" + thisThreadId, m_objectMapper);
			}
		}

		m_root = m_objectMapper.createObjectNode();
		m_root.put(key, value);
	}

	public DComputeContext(String key, boolean value) {
		long thisThreadId = Thread.currentThread().getId();
		synchronized(m_mapThreadIdOM) {
			if (m_mapThreadIdOM.containsKey("" + thisThreadId))
				m_objectMapper = m_mapThreadIdOM.get("" + thisThreadId);
			else {
				m_objectMapper = new ObjectMapper();
				m_mapThreadIdOM.put("" + thisThreadId, m_objectMapper);
			}
		}

		m_root = m_objectMapper.createObjectNode();
		m_root.put(key, value);
	}

	public String getString(String field) {
		return m_root.get(field).asText();
	}
	
	public int getInt(String field) {
		return m_root.get(field).asInt();
	}

	public long getLong(String field) {
		return m_root.get(field).asLong();
	}

	public double getDouble(String field) {
		return m_root.get(field).asDouble();
	}

	public boolean getBoolean(String field) {
		return m_root.get(field).asBoolean();
	}
	
	public Iterator<String> fieldNames() {
		return m_root.fieldNames();
	}
	
	public String toString() {
		if (m_isChanged) {
			try {
				m_jsonString = m_objectMapper.writeValueAsString(m_root);
				m_isChanged = false;
			} catch (JsonProcessingException e) {
				throw new RuntimeException(e);
			}
		}
		//log.debug("toString() jsonString : {}", m_jsonString);
		return m_jsonString;
	}
	
	public DComputeContext add(String key, String value) {
		m_root.put(key, value);
		m_isChanged = true;
		m_isDsonEmpty = false;
		return this;
	}
	
	public DComputeContext add(String key, int value) {
		m_root.put(key, value);
		m_isChanged = true;
		m_isDsonEmpty = false;
		return this;
	}
	
	public DComputeContext add(String key, long value) {
		m_root.put(key, value);
		m_isChanged = true;
		m_isDsonEmpty = false;
		return this;
	}

	public DComputeContext add(String key, float value) {
		m_root.put(key, value);
		m_isChanged = true;
		m_isDsonEmpty = false;
		return this;
	}

	public DComputeContext add(String key, double value) {
		m_root.put(key, value);
		m_isChanged = true;
		m_isDsonEmpty = false;
		return this;
	}
	
	public DComputeContext add(String key, boolean value) {
		m_root.put(key, value);
		m_isChanged = true;
		m_isDsonEmpty = false;
		return this;
	}

	private static DComputeContext createDsonList(HashMap<String, String> hmap, LinkedList<String> fieldNames) {
	    DComputeContext job = new DComputeContext();

	    if (true == hmap.isEmpty()) {
	    	return null;
	    }
	    if (true == fieldNames.isEmpty()) {
	    	return null;
	    }
	    for (String field : fieldNames) {
	    	//System.out.println("createDsonList() from hmap {}" + hmap.get(field) + " field " + field);
	    	//log.debug("createDsonList() from hmap {} field {}", hmap.get(field), field);

	    	job.add(field, hmap.get(field));
	    }
		
		return job;
		
	}
	
	private static DComputeContext createDsonSet(HashMap<String, String> hmap, Set<String> st) {
	    DComputeContext job = new DComputeContext();

	    if (true == hmap.isEmpty()) {
	    	return null;
	    }
	    if (true == st.isEmpty()) {
	    	return null;
	    }
	    for (String field : st) {
	    	//System.out.println("createDsonSet() from hmap {}" + hmap.get(field) + " field " + field);
	    	//log.debug("createDsonSet() from hmap {} field {}", hmap.get(field), field);

	    	job.add(field, hmap.get(field));
	    }
		
		return job;
		
	}
		
	private static DComputeContext createDson(LinkedList<String> arrayofDJsons) {
	    DComputeContext job = new DComputeContext();
	    long n = 1;
	    for (String s : arrayofDJsons) {
	    	//log.debug("createDson() jsonString : {}", s);
	        //System.out.println("createDson() jsonString : " + s);
	    	job.add("" + n, s);
	    	n++;
	    }
		
		return job;
		
	}
	
	private static DComputeContext createDsonWithCount(LinkedList<String> arrayofDJsons) {
	    DComputeContext job = new DComputeContext();

	    // Previous works String count = "" + arrayofDJsons.size();
	    // Previous works job.add("Count", "" + count);
	    long n = 1;
	    for (String s : arrayofDJsons) {
	    	//log.debug("createDsonWithCount() jsonString : {}", s);
	        //System.out.println("createDsonWithCount() jsonString : " + s);
	    	job.add("" + n, s);
	    	n++;
	    }
	    String sCount = "" + (n - 1);
	    job.add("Count", sCount);
	    
		return job;
		
	}
	
	public static String ok() {
		return "{ \"Ok\" : \"1\" }";
	}

	public static String empty() {
		return "{ \"Empty\" : \"1\" }";
	}

	public static String asTrue() {
		return "{ \"True\" : \"1\" }";
	}

	public static String asFalse() {
		return "{ \"False\" : \"1\" }";
	}

	public boolean isTrue() throws IOException {
		if (1 == m_root.size() && m_root.has("True"))
			return true;
		else
			return false;
	}
	
	public boolean isFalse() throws IOException {
		if (1 == m_root.size() && m_root.has("False"))
			return true;
		else
			return false;
	}
	
	public static String error(String errorMessage) {
		//return "{ \"Error\" : \"" + errorMessage + "\" }";
		return (new DComputeContext("Error", errorMessage)).toString();
	}
	
	public static String result(String result) {
		//return "{ \"Result\" : \"" + result + "\" }";
		return (new DComputeContext("Result", result)).toString();
	}

	public boolean isOk() throws IOException {
		if (1 == m_root.size() && m_root.has("Ok"))
			return true;
		else
			return false;
	}

	public boolean isError() throws IOException {
		if (1 == m_root.size() && m_root.has("Error"))
			return true;
		else
			return false;
	}
	
	public boolean isEmpty() throws IOException {
		if (1 == m_root.size() && m_root.has("Empty"))
			return true;
		else if (true == m_isDsonEmpty)
			return true;
		else
			return false;
	}

	public boolean isResult() throws IOException {
		if (1 == m_root.size() && m_root.has("Result"))
			return true;
		else
			return false;
	}

	public long getTU() {
		return getLongOf("TotalComputeUnit");
	}

	public long getCU() {
		return getLongOf("SplitComputeUnit");
	}
	
	public int getIntOf(String field) {
		return Integer.parseInt(getString(field));
	}
	
	public long getLongOf(String field) {
		return Long.parseLong(getString(field));
	}

	public float getFloatOf(String field) {
		return Float.parseFloat(getString(field));
	}

	public double getDoubleOf(String field) {
		return Double.parseDouble(getString(field));
	}

	public boolean getBooleanOf(String field) {
		return Boolean.parseBoolean(getString(field));
	}
	
	public Dson getInput() throws IOException {
		return new Dson(getString("JsonInput"));
	}
	
	public DComputeContext getResult() throws IOException {
		return new DComputeContext(getString("Result"));
	}

	public long getCount() throws IOException {
		return Long.parseLong(getString("Count"));
	}

	public boolean contains(String key) {
		return m_root.has(key);
	}
	
	public static DComputeContext dummy() throws IOException {
		DComputeContext dson1 = new DComputeContext("TotalComputeUnit", "0");
		DComputeContext dson2 = dson1.add("SplitComputeUnit", "0");
		DComputeContext dson3 = dson2.add("JsonInput", DComputeContext.empty());

		return dson3;
	}

	public static String dummyAsString() throws IOException {
		DComputeContext dson1 = new DComputeContext("TotalComputeUnit", "0");
		DComputeContext dson2 = dson1.add("SplitComputeUnit", "0");
		DComputeContext dson3 = dson2.add("JsonInput", DComputeContext.empty());

		return dson3.toString();
	}

}


//==============================

/* Previous works
public class Dson {

	private static final Logger log = LoggerFactory.getLogger(Dson.class);
	private String m_jsonString = null;
	private JsonObject m_jsonObject = null;
	private boolean isDsonEmpty = false;
	
	public Dson(String jsonString) throws IOException {
		m_jsonString = jsonString;
		InputStream fis = new ByteArrayInputStream(Charset.forName("UTF-16").encode(jsonString).array());
		
		JsonReader jsonReader = Json.createReader(fis);
		
		//Reference - create using factory
		//JsonReaderFactory factory = Json.createReaderFactory(null);
		//jsonReader = factory.createReader(fis);
		
		
		m_jsonObject = jsonReader.readObject();
		
		jsonReader.close();
		fis.close();

	}
	
	public Dson(JsonObject jsonObject) {
		
		m_jsonObject = jsonObject;
		m_jsonString = jsonObject.toString();
	}

	public Dson() {
		
	    JsonObjectBuilder job = Json.createObjectBuilder();

    	job.add("Empty", "1");
	    JsonObject j = job.build();
		
		m_jsonObject = j;
		m_jsonString = j.toString();

		isDsonEmpty = true;
	}

	public Dson(String key, String value) {
	    JsonObjectBuilder job = Json.createObjectBuilder();

    	job.add(key, value);
	    JsonObject j = job.build();
		
		m_jsonObject = j;
		m_jsonString = j.toString();
		
	}
	
	public Dson(String key, int value) {
	    JsonObjectBuilder job = Json.createObjectBuilder();

    	job.add(key, value);
	    JsonObject j = job.build();
		
		m_jsonObject = j;
		m_jsonString = j.toString();
		
	}
	
	public Dson(String key, long value) {
	    JsonObjectBuilder job = Json.createObjectBuilder();

    	job.add(key, value);
	    JsonObject j = job.build();
		
		m_jsonObject = j;
		m_jsonString = j.toString();
		
	}
	
	public Dson(String key, float value) {
	    JsonObjectBuilder job = Json.createObjectBuilder();

    	job.add(key, value);
	    JsonObject j = job.build();
		
		m_jsonObject = j;
		m_jsonString = j.toString();
		
	}

	public Dson(String key, double value) {
	    JsonObjectBuilder job = Json.createObjectBuilder();

    	job.add(key, value);
	    JsonObject j = job.build();
		
		m_jsonObject = j;
		m_jsonString = j.toString();
		
	}

	public Dson(String key, boolean value) {
	    JsonObjectBuilder job = Json.createObjectBuilder();

    	job.add(key, value);
	    JsonObject j = job.build();
		
		m_jsonObject = j;
		m_jsonString = j.toString();
		
	}

	public String getString(String field) {
		return m_jsonObject.getString(field);
	}
	
	public int getInt(String field) {
		return m_jsonObject.getInt(field);
	}

	public JsonNumber getNumber(String field) {
		return m_jsonObject.getJsonNumber(field);
	}

	public boolean getBoolean(String field) {
		return m_jsonObject.getBoolean(field);
	}
	
	public JsonArray getJsonArray(String field) {
		return m_jsonObject.getJsonArray(field);
	}
	public JsonObject getJsonObject(String field) {
		return m_jsonObject.getJsonObject(field);
	}
	
	public Set<String> keySet() {
		return m_jsonObject.keySet();
	}
	
	public String toString() {
		//log.debug("toString() jsonString : {}", m_jsonString);
		return m_jsonString;
	}
	
	public Dson add(String key, String value) {
		JsonObjectBuilder job = Json.createObjectBuilder();

		if (false == isDsonEmpty) {
			for (Entry<String, JsonValue> entry : m_jsonObject.entrySet()) {
				job.add(entry.getKey(), entry.getValue());
			}
		}
		    
		JsonObject j = job.add(key, value).build();
		
		m_jsonObject = j;
		m_jsonString = j.toString();
		isDsonEmpty = false;
		return this;
	}
	
	public Dson add(String key, int value) {
	    JsonObjectBuilder job = Json.createObjectBuilder();

	    if (false == isDsonEmpty) {
		    for (Entry<String, JsonValue> entry : m_jsonObject.entrySet()) {
		        job.add(entry.getKey(), entry.getValue());
		    }
	    }
	    JsonObject j = job.add(key, value).build();
	
		m_jsonObject = j;
		m_jsonString = j.toString();
		isDsonEmpty = false;
		return this;
	}
	
	public Dson add(String key, long value) {
	    JsonObjectBuilder job = Json.createObjectBuilder();

	    if (false == isDsonEmpty) {
		    for (Entry<String, JsonValue> entry : m_jsonObject.entrySet()) {
		        job.add(entry.getKey(), entry.getValue());
		    }
	    }
	    JsonObject j = job.add(key, value).build();
	
		m_jsonObject = j;
		m_jsonString = j.toString();
		isDsonEmpty = false;
		return this;
	}

	public Dson add(String key, float value) {
	    JsonObjectBuilder job = Json.createObjectBuilder();

	    if (false == isDsonEmpty) {
		    for (Entry<String, JsonValue> entry : m_jsonObject.entrySet()) {
		        job.add(entry.getKey(), entry.getValue());
		    }
	    }
	    JsonObject j = job.add(key, value).build();
	
		m_jsonObject = j;
		m_jsonString = j.toString();
		isDsonEmpty = false;
		return this;
	}

	public Dson add(String key, double value) {
	    JsonObjectBuilder job = Json.createObjectBuilder();

	    if (false == isDsonEmpty) {
		    for (Entry<String, JsonValue> entry : m_jsonObject.entrySet()) {
		        job.add(entry.getKey(), entry.getValue());
		    }
	    }
	    JsonObject j = job.add(key, value).build();
	
		m_jsonObject = j;
		m_jsonString = j.toString();
		isDsonEmpty = false;
		return this;
	}
	
	public Dson add(String key, boolean value) {
	    JsonObjectBuilder job = Json.createObjectBuilder();

	    if (false == isDsonEmpty) {
		    for (Entry<String, JsonValue> entry : m_jsonObject.entrySet()) {
		        job.add(entry.getKey(), entry.getValue());
		    }
	    }
	    JsonObject j = job.add(key, value).build();
	
		m_jsonObject = j;
		m_jsonString = j.toString();
		isDsonEmpty = false;
		return this;
	}

	private static Dson createDsonList(HashMap<String, String> hmap, ArrayList<String> fieldNames) {
	    JsonObjectBuilder job = Json.createObjectBuilder();

	    if (true == hmap.isEmpty()) {
	    	return null;
	    }
	    if (true == fieldNames.isEmpty()) {
	    	return null;
	    }
	    for (String field : fieldNames) {
	    	//System.out.println("createDsonList() from hmap {}" + hmap.get(field) + " field " + field);
	    	//log.debug("createDsonList() from hmap {} field {}", hmap.get(field), field);

	    	job.add(field, (String) hmap.get(field));
	    }
	    JsonObject j = job.build();
		
		return new Dson(j);
		
	}
	
	private static Dson createDsonSet(HashMap<String, String> hmap, Set<String> st) {
	    JsonObjectBuilder job = Json.createObjectBuilder();

	    if (true == hmap.isEmpty()) {
	    	return null;
	    }
	    if (true == st.isEmpty()) {
	    	return null;
	    }
	    for (String field : st) {
	    	//System.out.println("createDsonSet() from hmap {}" + hmap.get(field) + " field " + field);
	    	//log.debug("createDsonSet() from hmap {} field {}", hmap.get(field), field);

	    	job.add(field, (String) hmap.get(field));
	    }
	    JsonObject j = job.build();
		
		return new Dson(j);
		
	}
		
	private static Dson createDson(ArrayList<String> arrayofDJsons) {
	    JsonObjectBuilder job = Json.createObjectBuilder();
	    int n = 1;
	    for (String s : arrayofDJsons) {
	    	//log.debug("createDson() jsonString : {}", s);
	        //System.out.println("createDson() jsonString : " + s);
	    	job.add("" + n, s);
	    	n++;
	    }
	    JsonObject j = job.build();
		
		return new Dson(j);
		
	}
	
	private static Dson createDsonWithCount(ArrayList<String> arrayofDJsons) {
	    JsonObjectBuilder job = Json.createObjectBuilder();

	    String count = "" + arrayofDJsons.size();
	    job.add("Count", "" + count);
	    int n = 1;
	    for (String s : arrayofDJsons) {
	    	//log.debug("createDsonWithCount() jsonString : {}", s);
	        //System.out.println("createDsonWithCount() jsonString : " + s);
	    	job.add("" + n, s);
	    	n++;
	    }
	    JsonObject j = job.build();
		
		return new Dson(j);
		
	}
	
	public static String ok() {
		return "{ \"Ok\" : \"1\" }";
	}

	public static String empty() {
		return "{ \"Empty\" : \"1\" }";
	}

	public static String asTrue() {
		return "{ \"True\" : \"1\" }";
	}

	public static String asFalse() {
		return "{ \"False\" : \"1\" }";
	}

	public boolean isTrue() throws IOException {
		
		Set<String> st = m_jsonObject.keySet();
		
		if (1 == st.size() && st.contains("True"))
			return true;
		else
			return false;
	}
	
	public boolean isFalse() throws IOException {
		
		Set<String> st = m_jsonObject.keySet();
		
		if (1 == st.size() && st.contains("False"))
			return true;
		else
			return false;
	}
	
	public static String error(String errorMessage) {
		//return "{ \"Error\" : \"" + errorMessage + "\" }";
		return (new Dson("Error", errorMessage)).toString();
	}
	
	public static String result(String result) {
		//return "{ \"Result\" : \"" + result + "\" }";
		return (new Dson("Result", result)).toString();
	}

	public boolean isOk() throws IOException {
		
		Set<String> st = m_jsonObject.keySet();
		
		if (1 == st.size() && st.contains("Ok"))
			return true;
		else
			return false;
	}

	public boolean isError() throws IOException {
		
		Set<String> st = m_jsonObject.keySet();
		
		if (1 == st.size() && st.contains("Error"))
			return true;
		else
			return false;
	}
	
	public boolean isEmpty() throws IOException {
		
		Set<String> st = m_jsonObject.keySet();
		
		if (1 == st.size() && st.contains("Empty"))
			return true;
		else if (m_jsonObject.isEmpty())
			return true;
		else
			return false;
	}

	public boolean isResult() throws IOException {
		
		Set<String> st = m_jsonObject.keySet();
		
		if (1 == st.size() && st.contains("Result"))
			return true;
		else if (m_jsonObject.isEmpty())
			return true;
		else
			return false;
	}

	public int getTU() {
		return getIntOf("TotalComputeUnit");
	}

	public int getCU() {
		return getIntOf("SplitComputeUnit");
	}
	
	public int getIntOf(String field) {
		return Integer.parseInt(getString(field));
	}
	
	public long getLongOf(String field) {
		return Long.parseLong(getString(field));
	}

	public float getFloatOf(String field) {
		return Float.parseFloat(getString(field));
	}

	public double getDoubleOf(String field) {
		return Double.parseDouble(getString(field));
	}

	public boolean getBooleanOf(String field) {
		return Boolean.parseBoolean(getString(field));
	}
	
	public Dson getInput() throws IOException {
		return new Dson(getString("JsonInput"));
	}
	
	public Dson getResult() throws IOException {
		return new Dson(getString("Result"));
	}

	public int getCount() throws IOException {
		return Integer.parseInt(getString("Count"));
	}

	public boolean contains(String key) {
		return m_jsonObject.keySet().contains(key);
	}
	
	public static Dson dummyDson() throws IOException {
		Dson dson1 = new Dson("TotalComputeUnit", "1");
		Dson dson2 = dson1.add("SplitComputeUnit", "1");
		Dson dson3 = dson2.add("JsonInput", Dson.empty());

		return dson3;
	}

	public static String dummy() throws IOException {
		Dson dson1 = new Dson("TotalComputeUnit", "1");
		Dson dson2 = dson1.add("SplitComputeUnit", "1");
		Dson dson3 = dson2.add("JsonInput", Dson.empty());

		return dson3.toString();
	}

}
*/

