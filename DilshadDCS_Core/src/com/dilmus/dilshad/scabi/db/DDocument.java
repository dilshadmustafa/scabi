/**
 * @author Dilshad Mustafa
 * Copyright (c) Dilshad Mustafa
 * All Rights Reserved.
 * Created 06-Mar-2016
 * File Name : DDocument.java
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

package com.dilmus.dilshad.scabi.db;

import java.util.Date;
import java.util.Set;

import org.bson.BasicBSONObject;
import org.bson.BsonDocument;
import org.bson.Document;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

/**
 * @author Dilshad Mustafa
 *
 */
public class DDocument {

	private Document m_document = null;
	
	public DDocument() {
		m_document = new Document();
	}
	
	public DDocument(Document document) {
		m_document = document;
	}
	
	public int set(Document document) {
		m_document = document;
		return 0;
	}
	
	public int clear() {
		m_document.clear();
		return 0;
	}
	
	public int put(String key, String value) {
		m_document.put(key, value);
		return 0;
	}
	
	public int put(String key, int value) {
		m_document.put(key, value);
		return 0;
	}

	public int put(String key, long value) {
		m_document.put(key, value);
		return 0;
	}

	public int put(String key, float value) {
		m_document.put(key, value);
		return 0;
	}

	public int put(String key, double value) {
		m_document.put(key, value);
		return 0;
	}

	public int put(String key, Date value) {
		m_document.put(key, value);
		return 0;
	}

	public int put(String key, Document value) {
		m_document.put(key, value);
		return 0;
	}
	
	public int put(String key, DDocument value) {
		m_document.put(key, value.getDocument());
		return 0;
	}
	
	public DDocument append(String key, String value) {
		m_document.put(key, value);
		return this;
	}
	
	public DDocument append(String key, int value) {
		m_document.put(key, value);
		return this;
	}

	public DDocument append(String key, long value) {
		m_document.put(key, value);
		return this;
	}

	public DDocument append(String key, float value) {
		m_document.put(key, value);
		return this;
	}

	public DDocument append(String key, double value) {
		m_document.put(key, value);
		return this;
	}

	public DDocument append(String key, Date value) {
		m_document.put(key, value);
		return this;
	}

	public DDocument append(String key, Document value) {
		m_document.put(key, value);
		return this;
	}
	
	public DDocument append(String key, DDocument value) {
		m_document.put(key, value.getDocument());
		return this;
	}

	public String getString(String key) {
		return m_document.getString(key);
	}
	
	public Integer getInteger(String key) {
		return m_document.getInteger(key);
	}

	public Long getLong(String key) {
		return m_document.getLong(key);
	}

	public Double getDouble(String key) {
		return m_document.getDouble(key);
	}

	public Date getDate(String key) {
		return m_document.getDate(key);
	}
	
	public Object get(String key) {
		return m_document.get(key);
	}
	
	public Set<String> keySet() {
		return m_document.keySet();
	}
	
	public Document getDocument() {
		return m_document;
	}
	
	public String toString() {
		return m_document.toString();
	}
}

