/**
 * @author Dilshad Mustafa
 * Copyright (c) Dilshad Mustafa
 * All Rights Reserved.
 * Created 27-Jan-2016
 * File Name : DMJson.java
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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map.Entry;
import java.util.Set;

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

public class DMJson {

	private static final Logger log = LoggerFactory.getLogger(DMJson.class);
	private static final HashMap<String, ObjectMapper> m_mapThreadIdOM = new HashMap<String, ObjectMapper>();
	private ObjectMapper m_objectMapper = null;
	private ObjectNode m_root = null;
	private boolean m_isChanged = true;

	private String m_jsonString = null;
	private boolean m_isDsonEmpty = false;
	
	public DMJson(String jsonString) throws IOException {
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
	
	public DMJson() {
		
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

	public DMJson(String key, String value) {
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
	
	public DMJson(String key, int value) {
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
	
	public DMJson(String key, long value) {
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

	public DMJson(String key, float value) {
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

	public DMJson(String key, double value) {
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

	public DMJson(String key, boolean value) {
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

	public int clear() {
		m_root.removeAll();
		m_isChanged = true;
		m_jsonString = null;
		
		return 0;
	}
	
	public int set(String jsonString) throws IOException {
		m_root = (ObjectNode) m_objectMapper.readTree(jsonString);
		m_jsonString = jsonString;		
		m_isChanged = false;
		return 0;
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
	
	public Set<String> keySet() {
		Set<String> st = new HashSet<String>();
		Iterator<String> itr = m_root.fieldNames();
		while (itr.hasNext()) {
			st.add(itr.next());
		}
		return st;
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
	
	public DMJson remove(String key) {
		m_root.remove(key);
		m_isChanged = true;
		if (0 == m_root.size())
			m_isDsonEmpty = true;
		else
			m_isDsonEmpty = false;
		return this;
	}
	
	public DMJson add(String key, String value) {
		m_root.put(key, value);
		m_isChanged = true;
		m_isDsonEmpty = false;
		return this;
	}
	
	public DMJson add(String key, int value) {
		m_root.put(key, value);
		m_isChanged = true;
		m_isDsonEmpty = false;
		return this;
	}
	
	public DMJson add(String key, long value) {
		m_root.put(key, value);
		m_isChanged = true;
		m_isDsonEmpty = false;
		return this;
	}
	
	public DMJson add(String key, float value) {
		m_root.put(key, value);
		m_isChanged = true;
		m_isDsonEmpty = false;
		return this;
	}

	public DMJson add(String key, double value) {
		m_root.put(key, value);
		m_isChanged = true;
		m_isDsonEmpty = false;
		return this;
	}

	public DMJson add(String key, boolean value) {
		m_root.put(key, value);
		m_isChanged = true;
		m_isDsonEmpty = false;
		return this;
	}

	public static DMJson createDJsonList(HashMap<String, String> hmap, LinkedList<String> fieldNames) {
	    DMJson job = new DMJson();

	    if (true == hmap.isEmpty()) {
	    	return null;
	    }
	    if (true == fieldNames.isEmpty()) {
	    	return null;
	    }
	    for (String field : fieldNames) {
	    	//System.out.println("createDMJsonList() from hmap {}" + hmap.get(field) + " field " + field);
	    	//log.debug("createDMJsonList() from hmap {} field {}", hmap.get(field), field);

	    	job.add(field, hmap.get(field));
	    }
		
		return job;
	}
	
	public static DMJson createDJsonSet(HashMap<String, String> hmap, Set<String> st) {
	    DMJson job = new DMJson();

	    if (true == hmap.isEmpty()) {
	    	return null;
	    }
	    if (true == st.isEmpty()) {
	    	return null;
	    }
	    for (String field : st) {
	    	//System.out.println("createDMJsonSet() from hmap {}" + hmap.get(field) + " field " + field);
	    	//log.debug("createDMJsonSet() from hmap {} field {}", hmap.get(field), field);

	    	job.add(field, hmap.get(field));
	    }
		
		return job;
	}
		
	public static DMJson createDJson(LinkedList<String> arrayofDJsons) {
	    DMJson job = new DMJson();
	    long n = 1;
	    for (String s : arrayofDJsons) {
	    	//log.debug("createDMJson() jsonString : {}", s);
	        //System.out.println("createDMJson() jsonString : " + s);
	    	job.add("" + n, s);
	    	n++;
	    }
		
		return job;
	}
	
	public static DMJson createDJsonWithCount(LinkedList<String> arrayofDJsons) {
	    DMJson job = new DMJson();

	    // Previous works String count = "" + arrayofDJsons.size();
	    // Previous works job.add("Count", "" + count);
	    long n = 1;
	    for (String s : arrayofDJsons) {
	    	//log.debug("createDMJsonWithCount() jsonString : {}", s);
	        //System.out.println("createDMJsonWithCount() jsonString : " + s);
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
		return (new DMJson("Error", errorMessage)).toString();
	}

	public static String error(String errorCode, String errorMessage) {
		DMJson djson = new DMJson("ErrorCode", errorCode);
		djson.add("Error", errorMessage);
		return djson.toString();
	}	
	
	public static String result(String result) {
		//return "{ \"Result\" : \"" + result + "\" }";
		return (new DMJson("Result", result)).toString();
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

	public DMJson getInput() throws IOException {
		return new DMJson(getString("JsonInput"));
	}

	public DMJson getResult() throws IOException {
		return new DMJson(getString("Result"));
	}

	public long getCount() throws IOException {
		return Long.parseLong(getString("Count"));
	}

	public boolean contains(String key) {
		return m_root.has(key);
	}
	
	public static DMJson dummy() throws IOException {
		DMJson dson1 = new DMJson("TotalComputeUnit", "0");
		DMJson dson2 = dson1.add("SplitComputeUnit", "0");
		DMJson dson3 = dson2.add("JsonInput", DMJson.empty());

		return dson3;
	}

	public static String dummyAsString() throws IOException {
		DMJson dson1 = new DMJson("TotalComputeUnit", "0");
		DMJson dson2 = dson1.add("SplitComputeUnit", "0");
		DMJson dson3 = dson2.add("JsonInput", DMJson.empty());

		return dson3.toString();
	}

}

//============================

/* Previous works
public class DMJson {

	private static final Logger log = LoggerFactory.getLogger(DMJson.class);
	private String m_jsonString;
	private JsonObject m_jsonObject;
	
	public DMJson(String jsonString) throws IOException {
		m_jsonString = jsonString;
		InputStream fis = new ByteArrayInputStream(Charset.forName("UTF-16").encode(jsonString).array());
		//create JsonReader object
		JsonReader jsonReader = Json.createReader(fis);
		
		// To create JsonReader from factory
		// JsonReaderFactory factory = Json.createReaderFactory(null);
		// jsonReader = factory.createReader(fis);
		
		
		// get JsonObject from JsonReader
		m_jsonObject = jsonReader.readObject();
		
		jsonReader.close();
		fis.close();

	}
	
	public DMJson(JsonObject jsonObject) {
		
		m_jsonObject = jsonObject;
		m_jsonString = jsonObject.toString();
	}

	public DMJson(String key, String value) {
	    JsonObjectBuilder job = Json.createObjectBuilder();

    	job.add(key, value);
	    JsonObject j = job.build();
		
	    m_jsonObject = j;
	    m_jsonString = j.toString();
		
	}
	
	public DMJson(String key, int value) {
	    JsonObjectBuilder job = Json.createObjectBuilder();

    	job.add(key, value);
	    JsonObject j = job.build();
		
	    m_jsonObject = j;
	    m_jsonString = j.toString();
		
	}
	
	public DMJson(String key, long value) {
	    JsonObjectBuilder job = Json.createObjectBuilder();

    	job.add(key, value);
	    JsonObject j = job.build();
		
	    m_jsonObject = j;
	    m_jsonString = j.toString();
		
	}

	public DMJson(String key, float value) {
	    JsonObjectBuilder job = Json.createObjectBuilder();

    	job.add(key, value);
	    JsonObject j = job.build();
		
	    m_jsonObject = j;
	    m_jsonString = j.toString();
		
	}

	public DMJson(String key, double value) {
	    JsonObjectBuilder job = Json.createObjectBuilder();

    	job.add(key, value);
	    JsonObject j = job.build();
		
	    m_jsonObject = j;
	    m_jsonString = j.toString();
		
	}

	public DMJson(String key, boolean value) {
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
		//log.debug("toString() jsonString : {}", jsonString);
		return m_jsonString;
	}
	
	public DMJson add(String key, String value) {
		    JsonObjectBuilder job = Json.createObjectBuilder();

		    for (Entry<String, JsonValue> entry : m_jsonObject.entrySet()) {
		        job.add(entry.getKey(), entry.getValue());
		    }
		    JsonObject j = job.add(key, value).build();
		
		return new DMJson(j);
	}
	
	public DMJson add(String key, int value) {
	    JsonObjectBuilder job = Json.createObjectBuilder();

	    for (Entry<String, JsonValue> entry : m_jsonObject.entrySet()) {
	        job.add(entry.getKey(), entry.getValue());
	    }
	    JsonObject j = job.add(key, value).build();
	
	    return new DMJson(j);
	}
	
	public DMJson add(String key, long value) {
	    JsonObjectBuilder job = Json.createObjectBuilder();

	    for (Entry<String, JsonValue> entry : m_jsonObject.entrySet()) {
	        job.add(entry.getKey(), entry.getValue());
	    }
	    JsonObject j = job.add(key, value).build();
	
	    return new DMJson(j);
	}
	
	public DMJson add(String key, float value) {
	    JsonObjectBuilder job = Json.createObjectBuilder();

	    for (Entry<String, JsonValue> entry : m_jsonObject.entrySet()) {
	        job.add(entry.getKey(), entry.getValue());
	    }
	    JsonObject j = job.add(key, value).build();
	
	    return new DMJson(j);
	}

	public DMJson add(String key, double value) {
	    JsonObjectBuilder job = Json.createObjectBuilder();

	    for (Entry<String, JsonValue> entry : m_jsonObject.entrySet()) {
	        job.add(entry.getKey(), entry.getValue());
	    }
	    JsonObject j = job.add(key, value).build();
	
	    return new DMJson(j);
	}

	public DMJson add(String key, boolean value) {
	    JsonObjectBuilder job = Json.createObjectBuilder();

	    for (Entry<String, JsonValue> entry : m_jsonObject.entrySet()) {
	        job.add(entry.getKey(), entry.getValue());
	    }
	    JsonObject j = job.add(key, value).build();
	
	    return new DMJson(j);
	}

	public static DMJson createDJsonList(HashMap<String, String> hmap, ArrayList<String> fieldNames) {
	    JsonObjectBuilder job = Json.createObjectBuilder();

	    if (true == hmap.isEmpty()) {
	    	return null;
	    }
	    if (true == fieldNames.isEmpty()) {
	    	return null;
	    }
	    for (String field : fieldNames) {
	    	//System.out.println("createDJsonList(HashMap hmap, ArrayList<String> fieldNames) from hmap {}" + hmap.get(field) + " field " + field);
	    	//log.debug("createDJsonList(HashMap hmap, ArrayList<String> fieldNames) from hmap {} field {}", hmap.get(field), field);

	    	job.add(field, (String) hmap.get(field));
	    }
	    JsonObject j = job.build();
		
		return new DMJson(j);
		
	}
	
	public static DMJson createDJsonSet(HashMap<String, String> hmap, Set<String> st) {
	    JsonObjectBuilder job = Json.createObjectBuilder();

	    if (true == hmap.isEmpty()) {
	    	return null;
	    }
	    if (true == st.isEmpty()) {
	    	return null;
	    }
	    for (String field : st) {
	    	//System.out.println("createDJsonSet(HashMap hmap, ArrayList<String> fieldNames) from hmap {}" + hmap.get(field) + " field " + field);
	    	//log.debug("createDJsonSet(HashMap hmap, ArrayList<String> fieldNames) from hmap {} field {}", hmap.get(field), field);

	    	job.add(field, (String) hmap.get(field));
	    }
	    JsonObject j = job.build();
		
		return new DMJson(j);
		
	}
		
	public static DMJson createDJson(ArrayList<String> arrayofDJsons) {
	    JsonObjectBuilder job = Json.createObjectBuilder();
	    int n = 1;
	    for (String s : arrayofDJsons) {
	    	//log.debug("createDJson(ArrayList<String>) jsonString : {}", s);
	        //System.out.println("createDJson(ArrayList<String>) jsonString : " + s);
	    	job.add("" + n, s);
	    	n++;
	    }
	    JsonObject j = job.build();
		
		return new DMJson(j);
		
	}
	
	public static DMJson createDJsonWithCount(ArrayList<String> arrayofDJsons) {
	    JsonObjectBuilder job = Json.createObjectBuilder();

	    String count = "" + arrayofDJsons.size();
	    job.add("Count", ""+count);
	    int n = 1;
	    for (String s : arrayofDJsons) {
	    	//log.debug("createDJsonWithCount(ArrayList<String>) jsonString : {}", s);
	        //System.out.println("createDJsonWithCount(ArrayList<String>) jsonString : " + s);
	    	job.add("" + n, s);
	    	n++;
	    }
	    JsonObject j = job.build();
		
		return new DMJson(j);
		
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
		return (new DMJson("Error", errorMessage)).toString();
	}

	public static String result(String result) {
		//return "{ \"Result\" : \"" + result + "\" }";
		return (new DMJson("Result", result)).toString();
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

	public DMJson getInput() throws IOException {
		return new DMJson(getString("JsonInput"));
	}

	public DMJson getResult() throws IOException {
		return new DMJson(getString("Result"));
	}

	public long getCount() throws IOException {
		return Long.parseLong(getString("Count"));
	}

	public boolean contains(String key) {
		return m_jsonObject.keySet().contains(key);
	}
	
	public static DMJson dummyDson() throws IOException {
		DMJson dson1 = new DMJson("TotalComputeUnit", "1");
		DMJson dson2 = dson1.add("SplitComputeUnit", "1");
		DMJson dson3 = dson2.add("JsonInput", DMJson.empty());

		return dson3;
	}

	public static String dummy() throws IOException {
		DMJson dson1 = new DMJson("TotalComputeUnit", "1");
		DMJson dson2 = dson1.add("SplitComputeUnit", "1");
		DMJson dson3 = dson2.add("JsonInput", DMJson.empty());

		return dson3.toString();
	}

}
*/
