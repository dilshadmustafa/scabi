/**
 * @author Dilshad Mustafa
 * Copyright (c) Dilshad Mustafa
 * All Rights Reserved.
 * @since 11-Feb-2016
 * File Name : DObject.java
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
to another programming language and embedded modified versions of this Software source 
code and/or its compiled object binary in any form, both within as well as outside your 
organization, company, legal entity and/or individual. 

6. You should not embed any modification of this Software source code and/or its compiled 
object binary form in any way, either partially or fully.

7. You should not redistribute this Software, including its source code and/or its 
compiled object binary form, under differently named or renamed software. You should 
not publish this Software, including its source code and/or its compiled object binary 
form, modified or original, under your name or your company name or your product name. 
You should not sell this Software to any party, organization, company, legal entity 
and/or individual.

8. You agree fully to the terms and conditions of this License of this software product, 
under same software name and/or if it is renamed in future.

9. This software is created and programmed by Dilshad Mustafa and Dilshad holds the 
copyright for this Software and all its source code. You agree that you will not infringe 
or do any activity that will violate Dilshad's copyright of this software and all its 
source code.

10. THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
   EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
   MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
   NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHOR OR COPYRIGHT HOLDER
   BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
   ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
   CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
   SOFTWARE.

*/

package com.dilmus.dilshad.scabi.deprecated;

import java.util.Set;

import org.bson.BasicBSONObject;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

/**
 * @author Dilshad Mustafa
 *
 */
public class DObject {

	private DBObject m_document = null;
	
	public DObject() {
		m_document = new BasicDBObject();
	}
	
	public DObject(DBObject document) {
		m_document = document;
	}
	
	public int set(DBObject document) {
		m_document = document;
		return 0;
	}
	
	public int put(String key, String value) {
		m_document.put(key, value);
		return 0;
	}
	
	public int put(String key, DBObject value) {
		m_document.put(key, value);
		return 0;
	}
	
	public int put(String key, DObject value) {
		m_document.put(key, value.getDocument());
		return 0;
	}
	
	public DObject append(String key, String value) {
		m_document.put(key, value);
		return this;
	}
	
	public DObject append(String key, DBObject value) {
		m_document.put(key, value);
		return this;
	}
	
	public String getString(String key) {
		return (String) ((BasicBSONObject) m_document).getString(key);
	}
	
	public String get(String key) {
		return (String) ((BasicBSONObject) m_document).get(key);
	}
	
	public Set<String> keySet() {
		return m_document.keySet();
	}
	
	public DBObject getDocument() {
		return m_document;
	}
	
	public String toString() {
		return m_document.toString();
	}
}
