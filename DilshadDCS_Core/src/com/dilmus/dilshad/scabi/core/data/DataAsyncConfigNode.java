/**
 * @author Dilshad Mustafa
 * Copyright (c) Dilshad Mustafa
 * All Rights Reserved.
 * Created 18-Jul-2016
 * File Name : DataAsyncConfigNode.java
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

package com.dilmus.dilshad.scabi.core.data;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dilmus.dilshad.scabi.common.DMClassLoader;
import com.dilmus.dilshad.scabi.common.DMCounter;
import com.dilmus.dilshad.scabi.common.DMJson;
import com.dilmus.dilshad.scabi.common.DScabiException;
import com.dilmus.dilshad.scabi.core.DComputeUnit;
import com.dilmus.dilshad.scabi.core.DataUnit;

/**
 * @author Dilshad Mustafa
 *
 */
public class DataAsyncConfigNode {
	
	public final static int CNT_DATAUNIT_CONFIG = 1;
	public final static int CNT_PARTITIONER_CONFIG = 2;
	public final static int CNT_OPERATOR_CONFIG_1_1 = 3;
	public final static int CNT_OPERATOR_CONFIG_1_2 = 4;
	public final static int CNT_SHUFFLE_CONFIG_1_1 = 5;
	public final static int CNT_SHUFFLE_CONFIG_1_2 = 6;
	public final static int CNT_SHUFFLE_CONFIG_2_1 = 7;
	public final static int CNT_COMPARATOR_CONFIG_1_1 = 8;
	
	private final Logger log = LoggerFactory.getLogger(DataAsyncConfigNode.class);
	
	private DataUnitConfig m_dataUnitConfig = null;
	private DMPartitionerConfig m_partitionerConfig = null;
	private DMOperatorConfig_1_1 m_operatorConfig_1_1 = null;
	private DMOperatorConfig_1_2 m_operatorConfig_1_2 = null;
	private DMShuffleConfig_1_1 m_shuffleConfig_1_1 = null;
	private DMShuffleConfig_1_2 m_shuffleConfig_1_2 = null;
	private DMShuffleConfig_2_1 m_shuffleConfig_2_1 = null;
	private DMComparatorConfig_1_1 m_comparatorConfig = null;
	
	private int m_configNodeType = 0;
	
	private String m_jobId = null;
	private String m_configId = null;
	
	private static final DMCounter M_DMCOUNTER = new DMCounter();
	
	public int setJobId(String jobId) {
		m_jobId = jobId;
		m_configId = jobId + "_" + M_DMCOUNTER.inc();
		return 0;
	}
	
	public String getJobId() {
		return m_jobId;
	}

	public String getConfigId() {
		return m_configId;
	}

	public DataAsyncConfigNode(DataUnitConfig unit) {
		m_dataUnitConfig = unit;
		m_configNodeType = DataAsyncConfigNode.CNT_DATAUNIT_CONFIG;
		
		m_jobId = DMJson.empty();
		
		// Previous works m_configId = UUID.randomUUID().toString() + "_" + System.nanoTime() + "_" + M_DMCOUNTER.inc();
		// Previous works m_configId = m_configId.replace('-', '_');
		
		m_configId = DMJson.empty();
	}
	
	public DataAsyncConfigNode(DMPartitionerConfig unit) {
		m_partitionerConfig = unit;
		m_configNodeType = DataAsyncConfigNode.CNT_PARTITIONER_CONFIG;
		
		m_jobId = DMJson.empty();
		
		// Previous works m_configId = UUID.randomUUID().toString() + "_" + System.nanoTime() + "_" + M_DMCOUNTER.inc();
		// Previous works m_configId = m_configId.replace('-', '_');
		
		m_configId = DMJson.empty();
	}
		
	public DataAsyncConfigNode(DMOperatorConfig_1_1 unit) {
		m_operatorConfig_1_1 = unit;
		m_configNodeType = DataAsyncConfigNode.CNT_OPERATOR_CONFIG_1_1;
		
		m_jobId = DMJson.empty();
		
		// Previous works m_configId = UUID.randomUUID().toString() + "_" + System.nanoTime() + "_" + M_DMCOUNTER.inc();
		// Previous works m_configId = m_configId.replace('-', '_');
		
		m_configId = DMJson.empty();
	}

	public DataAsyncConfigNode(DMOperatorConfig_1_2 unit) {
		m_operatorConfig_1_2 = unit;
		m_configNodeType = DataAsyncConfigNode.CNT_OPERATOR_CONFIG_1_2;
		
		m_jobId = DMJson.empty();
		
		// Previous works m_configId = UUID.randomUUID().toString() + "_" + System.nanoTime() + "_" + M_DMCOUNTER.inc();
		// Previous works m_configId = m_configId.replace('-', '_');
		
		m_configId = DMJson.empty();
	}

	public DataAsyncConfigNode(DMShuffleConfig_1_1 unit) {
		m_shuffleConfig_1_1 = unit;
		m_configNodeType = DataAsyncConfigNode.CNT_SHUFFLE_CONFIG_1_1;
		
		m_jobId = DMJson.empty();
		
		// Previous works m_configId = UUID.randomUUID().toString() + "_" + System.nanoTime() + "_" + M_DMCOUNTER.inc();
		// Previous works m_configId = m_configId.replace('-', '_');
		
		m_configId = DMJson.empty();
	}
	
	public DataAsyncConfigNode(DMShuffleConfig_1_2 unit) {
		m_shuffleConfig_1_2 = unit;
		m_configNodeType = DataAsyncConfigNode.CNT_SHUFFLE_CONFIG_1_2;
		
		m_jobId = DMJson.empty();
		
		// Previous works m_configId = UUID.randomUUID().toString() + "_" + System.nanoTime() + "_" + M_DMCOUNTER.inc();
		// Previous works m_configId = m_configId.replace('-', '_');
		
		m_configId = DMJson.empty();
	}
	
	public DataAsyncConfigNode(DMShuffleConfig_2_1 unit) {
		m_shuffleConfig_2_1 = unit;
		m_configNodeType = DataAsyncConfigNode.CNT_SHUFFLE_CONFIG_2_1;
		
		m_jobId = DMJson.empty();
		
		// Previous works m_configId = UUID.randomUUID().toString() + "_" + System.nanoTime() + "_" + M_DMCOUNTER.inc();
		// Previous works m_configId = m_configId.replace('-', '_');
		
		m_configId = DMJson.empty();
	}

	public DataAsyncConfigNode(DMComparatorConfig_1_1 unit) {
		m_comparatorConfig = unit;
		m_configNodeType = DataAsyncConfigNode.CNT_COMPARATOR_CONFIG_1_1;
		
		m_jobId = DMJson.empty();
		
		// Previous works m_configId = UUID.randomUUID().toString() + "_" + System.nanoTime() + "_" + M_DMCOUNTER.inc();
		// Previous works m_configId = m_configId.replace('-', '_');
		
		m_configId = DMJson.empty();
	}
	
	public int getConfigNodeType() {
		return m_configNodeType;
	}
	
	public DataUnitConfig getDataUnitConfig() {
		return m_dataUnitConfig;
	}
	
	public DMPartitionerConfig getPartitionerConfig() {
		return m_partitionerConfig;
	}
	
	public DMOperatorConfig_1_1 getOperatorConfig_1_1() {
		return m_operatorConfig_1_1;
	}
	
	public DMOperatorConfig_1_2 getOperatorConfig_1_2() {
		return m_operatorConfig_1_2;
	}
	
	public DMShuffleConfig_1_1 getShuffleConfig_1_1() {
		return m_shuffleConfig_1_1;
	}
	
	public DMShuffleConfig_1_2 getShuffleConfig_1_2() {
		return m_shuffleConfig_1_2;
	}
	
	public DMShuffleConfig_2_1 getShuffleConfig_2_1() {
		return m_shuffleConfig_2_1;
	}
	
	public DMComparatorConfig_1_1 getComparatorConfig() {
		return m_comparatorConfig;
	}
	
}
