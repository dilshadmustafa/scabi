/**
 * @author Dilshad Mustafa
 * Copyright (c) Dilshad Mustafa
 * All Rights Reserved.
 * Created 21-Feb-2016
 * File Name : DMClassLoader.java
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
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Dilshad Mustafa
 *
 */
public class DMClassLoader extends ClassLoader {

	private final Logger log = LoggerFactory.getLogger(DMClassLoader.class);
	
	private HashMap<String, byte[]> m_mapClassNameClassBytes = new HashMap<String, byte[]>();
	private HashMap<String, byte[]> m_mapJarFilePathAsIDJarBytes = new HashMap<String, byte[]>();
	
	public HashMap<String, byte[]> getMapJarFilePathJarBytes() {
		return m_mapJarFilePathAsIDJarBytes;
	}
	public Class<?> findClass(String name, byte b[]) {
    	byte[] ba = b;

    	return defineClass(name,ba,0,ba.length);
    }
	
	// new custom method
	public Class<?> findClass(String name, byte b[], int len) {
    	byte[] ba = b;

    	return defineClass(name,ba,0,len);
    }

	public String loadJarAndSearchClass(String jarFilePathAsID, byte b[], String classNameToSearch) throws IOException {
		ByteArrayInputStream bais = new ByteArrayInputStream(b);
		JarInputStream jarStream = new JarInputStream(bais);

		JarEntry jarEntry;
		byte buffer[] = new byte[1024*1024];
		ByteArrayOutputStream baop = new ByteArrayOutputStream();
		int n = 0;
		String classNameToRun = classNameToSearch;
	    boolean isMatchFound = false;    
		while (true) {
			jarEntry = jarStream.getNextJarEntry();
			if (jarEntry == null) {
				break;
			}
			if(jarEntry.isDirectory() || !jarEntry.getName().endsWith(".class")){
				continue;
			}
			if (jarEntry.getName().endsWith(".class")) {
				String className = jarEntry.getName().substring(0,jarEntry.getName().length()-6);
				className = className.replace('/', '.');
				log.debug("loadJarAndSearchClass() className : {}", className);
				baop.reset();
				while( (n = jarStream.read(buffer, 0, buffer.length)) > 0)
					baop.write(buffer, 0, n);
				byte buffercls[] = baop.toByteArray();
				try {
					findClass(className, buffercls);
					m_mapClassNameClassBytes.put(className, buffercls);
				} catch (LinkageError e) {
					e.printStackTrace();
				} catch (Error | Exception e) {
					e.printStackTrace();
					jarStream.close();
					bais.close();
					baop.close();
					throw e;
				}
				if (false == isMatchFound && className.contains(classNameToRun)) {
					log.debug("loadJarAndSearchClass() found first matching name, classNameToRun : {}, className : {}", classNameToRun, className);
					classNameToRun = className;
					isMatchFound  = true;
					// Don't break here. All the classes in the jar should be loaded
				}
			}
		} 
		jarStream.close();
		bais.close();
		baop.close();
		if (isMatchFound) {
			m_mapJarFilePathAsIDJarBytes.put(jarFilePathAsID, b);
			return classNameToRun;
		}
		else
			return null;
	}
	
	public int loadJar(String jarFilePathAsID, byte b[]) throws IOException {

		ByteArrayInputStream bais = new ByteArrayInputStream(b);
		JarInputStream jarStream = new JarInputStream(bais);

		JarEntry jarEntry;
		byte buffer[] = new byte[1024*1024];
		ByteArrayOutputStream baop = new ByteArrayOutputStream();
		int n = 0;
	     
		while (true) {
			jarEntry = jarStream.getNextJarEntry();
			if (jarEntry == null) {
				break;
			}
			if(jarEntry.isDirectory() || !jarEntry.getName().endsWith(".class")){
				continue;
			}
			if (jarEntry.getName().endsWith(".class")) {
				String className = jarEntry.getName().substring(0,jarEntry.getName().length()-6);
				className = className.replace('/', '.');
				log.debug("loadJar() className : {}", className);
				baop.reset();
				while( (n = jarStream.read(buffer, 0, buffer.length)) > 0)
					baop.write(buffer, 0, n);
				byte buffercls[] = baop.toByteArray();
				try {
					findClass(className, buffercls);
					// Not used loadClass(className);
					m_mapClassNameClassBytes.put(className, buffercls);
				} catch (LinkageError e) {
					e.printStackTrace();
				} catch (Error | Exception e) {
					e.printStackTrace();
					jarStream.close();
					bais.close();
					baop.close();
					throw e;
				}
			}
		} 
		jarStream.close();
		bais.close();
		baop.close();
		m_mapJarFilePathAsIDJarBytes.put(jarFilePathAsID, b);
		return 0;
	}

	
	public InputStream getResourceAsStream(String classAsPath) {
		log.debug("getResourceAsStream() classAsPath  : {}", classAsPath);
		String className = null;
		
		// Previous works String className = classAsPath.replace(".class", "").replace('/', '.');
		
		if (classAsPath.endsWith(".class"))
			className = classAsPath.replace(".class", "").replace('/', '.');
		else
			className = classAsPath;
		
    	log.debug("getResourceAsStream() className  : {}", className);
    	
    	if (false == m_mapClassNameClassBytes.containsKey(className)) {
    		// Previous works return super.getResourceAsStream(className);
    		return super.getResourceAsStream(classAsPath);
    	}
		byte b[] = m_mapClassNameClassBytes.get(className);
		ByteArrayInputStream bais = new ByteArrayInputStream(b);
		return bais;
	}
}
