/**
 * @author Dilshad Mustafa
 * (c) Dilshad Mustafa
 * All Rights Reserved.
 * Created 12-Apr-2016
 * File Name : UnitTest7_DMJarUtil.java
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

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.annotation.Annotation;
import java.util.HashMap;
import java.util.concurrent.ExecutionException;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

import com.dilmus.dilshad.scabi.common.DScabiException;
import com.dilmus.dilshad.scabi.core.DComputeContext;
import com.dilmus.dilshad.scabi.core.DComputeUnit;
import com.dilmus.dilshad.scabi.core.DMeta;
import com.dilmus.dilshad.scabi.core.DataPartition;
import com.dilmus.dilshad.scabi.core.Dson;
import com.dilmus.dilshad.scabi.core.compute.DCompute;
import com.dilmus.dilshad.scabi.deprecated.DFieldGroup;
import com.dilmus.dilshad.scabi.common.DMUtil;
import com.dilmus.dilshad.scabi.common.DMClassLoader;
import com.dilmus.dilshad.scabi.common.DMJarUtil;

/**
 * @author Dilshad Mustafa
 *
 */
public class UnitTest7_DMJarUtil {

	public static void main(String args[]) throws Exception {
		
		DComputeUnit cu = new DComputeUnit() {
			class MyClass2 {
				
			}
			class MyClass5 {
				
			}
			public String compute(DComputeContext jsonInput) {
				DComputeUnit cu = new DComputeUnit() {
					class MyClass3 {
							class MyClass4 {
								
							}
					}
					public String compute(DComputeContext jsonInput) {
						
						return "Hello";
					}
				};

				return "Hello";
			}
		};
		
		
		//DMeta meta = new DMeta("localhost", "5000");
		//DCompute c = new DCompute(meta);
		//c.executeObject(cu).input(Dson.dummyDson());
		// error c.executeObject(jsonInput -> {return "Hello";}).input(Dson.dummyDson());
		//c.perform();
		//c.finish();
		//c.close();
		//meta.close();
		
		//Long lon = null;
		//System.out.println(lon instanceof Long);
		
		//Class<Long> lon = null;
		//Class<Integer> in = Integer.class;
		//in.asSubclass(Long.class);
		//System.out.println(lon..getCanonicalName());
		
		//File f = new File("testclass.jar");
		//f.createNewFile();
		
		//File tempFile=File.createTempFile("newJar",".jar");

		DMJarUtil.createJarFileUserClass(UnitTest7_DMJarUtil.class, "output.jar");
	}

	public static class MyClass {
		private int x = 0;
		private int y = 0;
		
		// jackson needs default constructor for dp.get(MyClass.class)
		public MyClass() {
			DComputeUnit cu = new DComputeUnit() {
				
				public String compute(DComputeContext jsonInput) {
					
					return "Hello";
				}
			};
		}
		
		public MyClass(int x, int y) {
			this.x = x;
			this.y = y;
		}
		
		public void setX(int x) {
			this.x = x;
		}
		
		public void setY(int y) {
			this.y = y;
		}
		
		public int getX() {
			return this.x;
		}
		
		public int getY() {
			return this.y;
		}
		
		public String toString() {
			String s = "MyClass obj => x: " + x + " y: " + y;
			
			return s;
		}
		
	}
	
}
