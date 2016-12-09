/**
 * @author Dilshad Mustafa
 * (c) Dilshad Mustafa
 * All Rights Reserved.
 * Created 23-Sep-2016
 * File Name : DMJarUtil.java
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
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Dilshad Mustafa
 *
 */
public class DMJarUtil {
	
	final static Logger log = LoggerFactory.getLogger(DMJarUtil.class);
	
	// Note : doesn't support inner class defined inside Anonymous class
	public static void createJarFileUserClasses(Iterable<Class<?>> itr, String filePath) throws IOException
	{
	  Manifest manifest = new Manifest();
	  manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
	  JarOutputStream target = new JarOutputStream(new FileOutputStream(filePath), manifest);
	  for (Class<?> cls : itr)
		  addJarUserClass(cls, target);
	  target.close();
	}
	
	
	// Note : doesn't support inner class defined inside Anonymous class
	public static void createJarFileUserClass(Class<?> cls, String filePath) throws IOException
	{
	  Manifest manifest = new Manifest();
	  manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
	  JarOutputStream target = new JarOutputStream(new FileOutputStream(filePath), manifest);
	  addJarUserClass(cls, target);
	  target.close();
	}

	public static void addJarUserClass(Class<?> cls, JarOutputStream target) throws IOException
	{
		// Difference between cls.getName() and cls.getCanonicalName()
		// cls.getName(); -> com.dilmus.test.Test2$MyClass
		// cls.getCanonicalName(); -> com.dilmus.test.Test2.MyClass
		
	  	String className = cls.getName();
	  	log.debug("addJarUserClass() className  : {}", className);
	  	System.out.println("addJarUserClass() className  : "+ className);
		String classAsPath = className.replace('.', '/') + ".class";
		log.debug("addJarUserClass() classAsPath  : {}", classAsPath);
		System.out.println("addJarUserClass() classAsPath  : " + classAsPath);
	
		InputStream in = cls.getClassLoader().getResourceAsStream(classAsPath);
		byte b[] = DMUtil.toBytesFromInStreamForJavaFiles(in);
		in.close();
	    ByteArrayInputStream bais = new ByteArrayInputStream(b);

		  try
		  {
		    JarEntry entry = new JarEntry(classAsPath);
		    target.putNextEntry(entry);
	
		    byte[] buffer = new byte[1024];
		    int count = 0;
		    while ((count = bais.read(buffer)) > 0)
		      target.write(buffer, 0, count);
		    target.closeEntry();
			bais.close();	    
		    
			Class<?>[] clsa = cls.getDeclaredClasses();
			  
		    for (Class<?> c : clsa) {
		    	log.debug("addJarUserClass() Declared Class is {}", c.getName());
		    	System.out.println("addJarUserClass() Declared Class is " + c.getName());
		    	log.debug("addJarUserClass() Declared Class, Canonical Name is {}", c.getCanonicalName());
		    	System.out.println("addJarUserClass() Declared Class, Canonical Name is " + c.getCanonicalName());
		    	addJarUserClass(c, target);
		    }
			
			DMClassLoader dm = new DMClassLoader();
			
			for (int i = 1; i <= 1000; i++) {
				String classAsPathi = className.replace('.', '/') + "$" + i + ".class";
				InputStream in2 = cls.getClassLoader().getResourceAsStream(classAsPathi);
				if (null == in2) {
					log.debug("addJarUserClass() Not Found : {}", classAsPathi);
					System.out.println("addJarUserClass() Not Found : " + classAsPathi);
					break;
				}
				else {
					log.debug("addJarUserClass() Found : {}", classAsPathi);
					System.out.println("addJarUserClass() Found : " + classAsPathi);
					String cName = className + "$" + i;
					log.debug("addJarUserClass() cName is {}", cName);
					System.out.println("addJarUserClass() cName is " + cName);
					addJarUserClass2(dm, cls, classAsPathi, cName, target);
				}
			} // End For
			
		  } catch (Exception e) {
			  // e.printStackTrace();
			  throw e;
		  }
	}	
	
	public static void addJarUserClass2(DMClassLoader dm, Class<?> origClass, String classAsPath, String canonicalName, JarOutputStream target) throws IOException
	{
 
		// Difference between cls.getName() and cls.getCanonicalName()
		// cls.getName(); -> com.dilmus.test.Test2$MyClass
		// cls.getCanonicalName(); -> com.dilmus.test.Test2.MyClass

		InputStream in = origClass.getClassLoader().getResourceAsStream(classAsPath);
		byte b[] = DMUtil.toBytesFromInStreamForJavaFiles(in);
		in.close();
	    ByteArrayInputStream bais = new ByteArrayInputStream(b);

		  try
		  {
		    JarEntry entry = new JarEntry(classAsPath);
		    target.putNextEntry(entry);
	
		    byte[] buffer = new byte[1024];
		    int count = 0;
		    while ((count = bais.read(buffer)) > 0)
		      target.write(buffer, 0, count);
		    target.closeEntry();
			bais.close();	    
		    
			//==========================
			
			/*
			// Note: As observed below, doesn't support inner class defined inside Anonymous class
			// cls.getDeclaredClasses() throws error java.lang.IllegalAccessError: tried to access class com.dilmus.test.Test2$1$MyClass2 from class com.dilmus.test.Test2$1
			
			Class<?> cls = dm.findClass(canonicalName, b);
			
			System.out.println("Declared classes for " + classAsPath);
			
			System.out.println("Class Name is " + cls.getName());
			// System.out.println("Class Name is " + cls.getCanonicalName()); // cls.getCanonicalName() is null

			try {
				Class<?>[] clsa = cls.getDeclaredClasses(); // java.lang.IllegalAccessError: tried to access class com.dilmus.test.Test2$1$MyClass2 from class com.dilmus.test.Test2$1
				cls.
			    for (Class<?> c : clsa) {
			    	System.out.println("Declared Class is " + c.getName());
			    	System.out.println("Declared Class, Canonical Name is " + c.getCanonicalName());
			    	// add(c, target);
			    }
			} catch (IllegalAccessError e) {
				e.printStackTrace();
				String s = e.getMessage();
				//String s2 = s.substring(beginIndex, endIndex)
				
			} catch (Error | Exception e) {
				e.printStackTrace();
			}
			
		    System.out.println("End declared classes for " + classAsPath);
			*/
			
			//==========================
			
			String nodotclassPath = classAsPath.substring(0, classAsPath.length() - 6);
			for (int i = 1; i <= 1000; i++) {
				String classAsPathi = nodotclassPath + "$" + i + ".class";
				InputStream in2 = origClass.getClassLoader().getResourceAsStream(classAsPathi);
				if (null == in2) {
					log.debug("addJarUserClass2() Not Found : {}", classAsPathi);
					System.out.println("addJarUserClass2() Not Found : " + classAsPathi);
					break;
				}
				else {
					log.debug("addJarUserClass2() Found : {}", classAsPathi);
					System.out.println("addJarUserClass2() Found : " + classAsPathi);
					String cName = canonicalName  + "$" + i; // or alternatively use nodotclassPath.replace('/', '.') + "$" + i;
					log.debug("addJarUserClass2() cName is {}", cName);
					System.out.println("addJarUserClass2() cName is " + cName);
					addJarUserClass2(dm, origClass, classAsPathi, cName, target);
				}
			} // End For
			
		  } catch (Exception e) {
			  // e.printStackTrace();
			  throw e;
		  }
	}		
	
}
