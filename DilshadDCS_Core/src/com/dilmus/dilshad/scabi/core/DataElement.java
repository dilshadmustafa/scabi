/**
 * @author Dilshad Mustafa
 * Copyright (c) Dilshad Mustafa
 * All Rights Reserved.
 * Created 15-Sep-2016
 * File Name : DataElement.java
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

import java.io.IOException;
import java.io.UnsupportedEncodingException;

import com.dilmus.dilshad.scabi.common.DMCounter;
import com.dilmus.dilshad.scabi.common.DScabiException;
//import com.fasterxml.jackson.core.JsonParseException;
//import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
//import com.leansoft.bigqueue.BigArrayImpl;
//import com.leansoft.bigqueue.IBigArray;

/**
 * @author Dilshad Mustafa
 *
 */
public class DataElement {

	// cw private static ObjectMapper m_objectMapper = new ObjectMapper();
	private ObjectMapper m_objectMapper = null;
	
	private byte m_bytea[] = null;
	private Dson m_dson = null;
	private Dson m_fieldsDson = null;
	private final static String M_DEFAULT_ENCODING = "UTF-8";
	
	private static DMCounter m_gcCounter = new DMCounter();
	
	private boolean m_isFieldsDsonSet = false;
	
	public int setJsonParser(ObjectMapper objectMapper) {
		m_objectMapper = objectMapper;
		return 0;
	}
	
	// Note this gc() is static, class level, as there are few objects created at object level per DataElement object level by various get...(...) methods below
	// for example by multiple invocations of getField(String fieldName), multiple String objects will be created

	// Note : next(), get() from DataPartition class and next() from DataPartitionIterator class already call gc()
	// so call to gc() in this DataElement class is not needed
	private static void gc() {
		m_gcCounter.inc();
		if (m_gcCounter.value() >= 100000) {
			System.gc();
			m_gcCounter.set(0);
		}
	}	
	
	// use default access, only classes of this package can access. For use by DataPartition class
	DataElement() throws DScabiException {
		m_bytea = null;
		m_dson = new Dson();
		m_fieldsDson = new Dson();
	}
	
	public DataElement(byte bytea[]) throws DScabiException {
		if (null == bytea)
			throw new DScabiException("Byte array is null", "DET.DET.1");
		m_bytea = bytea;
		m_dson = new Dson();
		m_fieldsDson = new Dson();
	}
	
	public int set(byte bytea[]) throws DScabiException {
		if (null == bytea)
			throw new DScabiException("Byte array is null", "DET.SET.1");
		m_bytea = bytea;
		m_isFieldsDsonSet = false;
		return 0;
	}
	
	public byte[] getBytes() {
		return m_bytea;
	}
	
	public String getString() throws UnsupportedEncodingException {
		return new String(m_bytea, M_DEFAULT_ENCODING);
	}

	public String getUnicodeString() {
		return new String(m_bytea);
	}

	public Dson getDson() throws IOException {
		String s = new String(m_bytea, M_DEFAULT_ENCODING);
		return new Dson(s);
	}
	
	public int getDson(Dson dson) throws IOException {
		String s = new String(m_bytea, M_DEFAULT_ENCODING);
		dson.set(s);
		return 0;
	}
	
	public <T> T get(Class<T> t) throws IOException {
		T tobj = m_objectMapper.readValue(m_bytea, t);
		return tobj;
	}
	
	public int get(IDsonInput d) throws Exception {
		String s = new String(m_bytea, M_DEFAULT_ENCODING);
		m_dson.set(s);
		d.set(m_dson);
		return 0;
	}
	
	// get...() for primary data types
	
	public int getInt() throws UnsupportedEncodingException {
		String s = new String(m_bytea, M_DEFAULT_ENCODING);
		int x = Integer.parseInt(s);
		return x;
	}
	
	public long getLong() throws UnsupportedEncodingException {
		String s = new String(m_bytea, M_DEFAULT_ENCODING);
		long x = Long.parseLong(s);
		return x;
	}
	
	public float getFloat() throws UnsupportedEncodingException {
		String s = new String(m_bytea, M_DEFAULT_ENCODING);
		float x = Float.parseFloat(s);
		return x;
	}

	public double getDouble() throws UnsupportedEncodingException {
		String s = new String(m_bytea, M_DEFAULT_ENCODING);
		double x = Double.parseDouble(s);
		return x;
	}

	public boolean getBoolean() throws UnsupportedEncodingException {
		String s = new String(m_bytea, M_DEFAULT_ENCODING);
		boolean x = Boolean.parseBoolean(s);
		return x;
	}
	
	// get...Field(...) methods for fields
	
	public String getField(String fieldName) throws IOException {
		if (false == m_isFieldsDsonSet) {
			String s = new String(m_bytea, M_DEFAULT_ENCODING);
			m_fieldsDson.set(s);
			
			m_isFieldsDsonSet = true;
		}
		String s2 = m_fieldsDson.getString(fieldName);
		return s2;
	}

	public int getIntField(String fieldName) throws IOException {
		if (false == m_isFieldsDsonSet) {
			String s = new String(m_bytea, M_DEFAULT_ENCODING);
			m_fieldsDson.set(s);
		
			m_isFieldsDsonSet = true;
		}
		int s2 = m_fieldsDson.getInt(fieldName);
		return s2;
	}

	public long getLongField(String fieldName) throws IOException {
		if (false == m_isFieldsDsonSet) {
			String s = new String(m_bytea, M_DEFAULT_ENCODING);
			m_fieldsDson.set(s);
		
			m_isFieldsDsonSet = true;
		}
		long s2 = m_fieldsDson.getLong(fieldName);
		return s2;
	}

	public double getDoubleField(String fieldName) throws IOException {
		if (false == m_isFieldsDsonSet) {
			String s = new String(m_bytea, M_DEFAULT_ENCODING);
			m_fieldsDson.set(s);
		
			m_isFieldsDsonSet = true;
		}
		double s2 = m_fieldsDson.getDouble(fieldName);
		return s2;
	}

	public boolean getBooleanField(String fieldName) throws IOException {
		if (false == m_isFieldsDsonSet) {
			String s = new String(m_bytea, M_DEFAULT_ENCODING);
			m_fieldsDson.set(s);
		
			m_isFieldsDsonSet = true;
		}
		boolean s2 = m_fieldsDson.getBoolean(fieldName);
		return s2;
	}

}
