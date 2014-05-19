/*
 * copyright 2013, gash
 * 
 * Gash licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package poke.server.management.managers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eye.Comm.LeaderElection;
import eye.Comm.Management;
import eye.Comm.Network;
import eye.Comm.Network.NetworkAction;
import eye.Comm.NetworkOrBuilder;
import poke.monitor.HeartMonitor;
import poke.monitor.MonitorHandler;
import poke.monitor.MonitorListener;
import poke.resources.ForwardResource;
import poke.server.Server;
import poke.server.conf.NodeDesc;

public class HeartbeatListener implements MonitorListener {
	protected static Logger logger = LoggerFactory.getLogger("management");

	private HeartbeatData data;
	public static boolean ifNodeCrashed=false;

	public HeartbeatListener(HeartbeatData data) {
		this.data = data;
	}

	public HeartbeatData getData() {
		return data;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see poke.monitor.MonitorListener#getListenerID()
	 */
	@Override
	public String getListenerID() {
		return data.getNodeId();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see poke.monitor.MonitorListener#onMessage(eye.Comm.Management)
	 */
	@Override
	public void onMessage(eye.Comm.Management msg) {
		
	System.out.println("msg.getBeat().getNodeId().equals(data.getNodeId()"+msg.getBeat().getNodeId());
		if (logger.isDebugEnabled())
			logger.debug(msg.getBeat().getNodeId());

		if (msg.hasGraph()) {
			logger.info("Received graph responses");
		} else if (msg.hasBeat() && msg.getBeat().getNodeId().equals(data.getNodeId())) {
			
			logger.info("Received HB response from " + msg.getBeat().getNodeId());
			//System.out.println("this is inside Heartbeat Listner class");
			data.setLastBeat(System.currentTimeMillis());
		} else{
			System.out.println("##########################################################################");
			logger.info("msg.getBeat.getNodeid"+msg.getBeat().getNodeId());
		logger.info("data.getnodeId "+data.getNodeId());
		logger.info("message has beat"+msg.hasBeat());
		logger.info("Equals"+msg.getBeat().getNodeId().equals(data.getNodeId()));
			logger.error("Received heartbeatMgr from on wrong channel or unknown host: " + msg.getBeat().getNodeId());
		}
		}

	@Override
	public void connectionClosed() {
		// note a closed management port is likely to indicate the primary port
		// has failed as well
		
		logger.info("the current leader is"+ElectionManager.LeaderNode);
		logger.info("node id is in heartbeatListner class"+this.getListenerID());
		
		//data.setFailures(Integer.parseInt(this.getListenerID()));
		
		
		
			
			
				
				int temp=Integer.parseInt(String.valueOf(this.getListenerID()))+1;
				
				System.out.println("value of temp is "+temp);
				String t=temp%4+"";
				
				String newNodeId="node.id"+temp%4;
				System.out.println("value of node.id is"+newNodeId);
				String newHost="host"+temp%4;
				System.out.println("value of new host is"+newHost);
				String newPort="port"+temp%4;
				System.out.println("value of new Port  is"+newPort);
				String newMgmtPort="mgmtPort"+temp%4;
				System.out.println("value of new mgmtPort is"+newMgmtPort);
				int currentport=Integer.parseInt(Server.allNodeConfDeatils.get("port"+getListenerID()));
			//	int currentportMgmt=Integer.parseInt(Server.allNodeConfDeatils.get("portmgmt"+getListenerID()));
				
				if(HeartbeatConnector.getInstance().stopListening(getListenerID(), currentport))
				{
					System.out.println("HAHAHAHAHAHA");
				}
				
				
				
				int nextPort=Integer.parseInt(Server.allNodeConfDeatils.get(newPort));
				System.out.println("value of nextPort is"+nextPort);
				int nextMgmtPort=Integer.parseInt(Server.allNodeConfDeatils.get(newMgmtPort));
				System.out.println("value oof nexMgmtport is"+nextMgmtPort);
				String nextHost=Server.allNodeConfDeatils.get(newHost);
				System.out.println("value of next Host is"+nextHost);
				HeartbeatManager.getInstance(temp%4+"");
				
				System.out.println();
				/*Network.Builder nb=Network.newBuilder();
				nb.setNodeId(getListenerID());
				nb.setAction(NetworkAction.NODELEAVE);
				Management.Builder m = Management.newBuilder();
				m.setGraph(nb);
				NetworkManager.getInstance(getListenerID()).processRequest(nb.build(), data.channel, data.sa);*/
				
				// send message to all the neigbours
				
			/*	NodeDesc nn=new NodeDesc();
				nn.setNodeId(temp%4+"");
				nn.setHost(nextHost);
				nn.setPort(nextPort);
				nn.setMgmtPort(nextMgmtPort);
				System.out.println("))))))))))))))))((((((((((((((((())))))))))))))))(((((((((((()))))))))))((((((");
				System.out.println("host"+nn.getHost());
				System.out.println("node id"+nn.getNodeId());
				System.out.println("port"+nn.getPort());
				System.out.println("mgmt port"+nn.getMgmtPort());
				
				*/
				
				System.out.println("new node id "+t);
				HeartbeatData node = new HeartbeatData(t, nextHost,nextPort,nextMgmtPort);
				System.out.println("data chanell is:"+data.channel);
				
			//	HeartMonitor hm=new HeartMonitor(data.getNodeId(), data.getHost(), data.getPort());
			//	hm.release();
			//	data.clearAll();
				/*node.setNodeId(newNodeId);
				node.setHost(nextHost);
				node.setPort(nextPort);
				node.setMgmtport(nextMgmtPort);
				*/
				
				/*data.setNodeId(newNodeId);
				data.setMgmtport(nextMgmtPort);
				data.setPort(nextPort);
				data.setHost(newHost);*/
				
				
				
				ifNodeCrashed=true;
				ElectionManager.IsLeaderElectionStarted=false;
				ElectionManager.isLeaderElected=false;
				logger.info("rEINITIATING LEADER ELECTION");
				
				
				
		
		HeartbeatConnector.getInstance().addConnectToThisNode(node);
		ElectionManager.getInstance(temp+"", 1).initiateElection();
		
		System.out.println("999999999999999999999999999999999999999999 heartbeat listner");
		
		
		
		
		
	}

	@Override
	public void connectionReady() {
		// do nothing at the moment
		System.out.println("&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&Heart beat listner");
	}
}
