/**
 * @author Dilshad Mustafa
 * Copyright (c) Dilshad Mustafa
 * All Rights Reserved.
 * @since 28-Jan-2016
 * File Name : DMDaoHelper.java
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

7. Under differently named or renamed software, you should not redistribute this 
Software and/or any modified works of this Software, including its source code 
and/or its compiled object binary form. Under your name or your company name or 
your product name, you should not publish this Software, including its source code 
and/or its compiled object binary form, modified or original. 

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

package com.dilmus.dilshad.scabi.common;

import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dilmus.dilshad.scabi.db.DDB;
import com.dilmus.dilshad.scabi.db.DTable;

/**
 * @author Dilshad Mustafa
 *
 */
public class DMDaoHelper {

	final static Logger log = LoggerFactory.getLogger(DMDaoHelper.class);
	private DDB m_ddb = null;
	
	public DMDaoHelper(String dbHost, String dbPort, String dbName) {
		m_ddb = new DDB(dbHost, dbPort, dbName);
	}
	
	public DMDaoHelper(DDB ddb) {
		m_ddb = ddb;
	}
	
	public DMDaoHelper() {
		m_ddb = null;
	}

	
	public int open(String dbHost, String dbPort, String dbName) {
		m_ddb = new DDB(dbHost, dbPort, dbName);
		return 0;
	}

	public int close() {
		if (m_ddb != null)
			m_ddb.close();
		return 0;
	}

	
	public DMDao createDAO() {
		return new DMDao(m_ddb);
	}
	
	public DTable getTable(String tableName) throws DScabiException {
		if (null == tableName) {
			throw new DScabiException("Table name is null", "DDH.GTE.1");
		}
		return m_ddb.getTable(tableName);
	}

	public DTable createTable(String tableName) throws DScabiException {
		if (null == tableName) {
			throw new DScabiException("Table name is null", "DDH.CTE.1");
		}
		return m_ddb.createTable(tableName);
	}
	
	/*
	public ArrayList<String> fieldNames(DTable table) throws DScabiException {
		if (null == table) {
			throw new DScabiException("Table is null", "DDH.FNS.1");
		}
		return m_ddb.fieldNamesUsingFindOne(table);
	}
	*/
	
	public boolean isEmpty(ArrayList<String> fieldList) {
		if (null == fieldList)
			return true;
		if (fieldList.isEmpty())
			return true;
		return false;
	}
	
}
