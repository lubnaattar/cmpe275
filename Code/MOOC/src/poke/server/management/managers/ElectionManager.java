/*
 * copyright 2014, gash
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

import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.GeneratedMessage;

import poke.demo.Monitor.HeartPrintListener;
import poke.monitor.*;
import poke.server.Server;
import poke.server.conf.NodeDesc;
import poke.server.conf.ServerConf;
import eye.Comm.LeaderElection;
import eye.Comm.LeaderElection.VoteAction;
import eye.Comm.Management;

/**
 * The election manager is used to determine leadership within the network.
 * 
 * @author gash
 * 
 */
public class ElectionManager {
	protected static Logger logger = LoggerFactory.getLogger("management");
	protected static AtomicReference<ElectionManager> instance = new AtomicReference<ElectionManager>();
	protected static String LeaderNode=null;
	private String nodeId;
	public TreeMap<String, String> allNodeDeatils = new TreeMap<String, String>();

	/** @brief the number of votes this server can cast */
	private int votes = 1;

	private ServerConf conf = null;

	protected static boolean isLeaderElected = false;
	protected static boolean IsLeaderElectionStarted = false;
	public static String currentNode = ""; // Currently connected node



	public static String getCurrentNode() {
		return currentNode;
	}

	public static void setCurrentNode(String node) {
		currentNode = node;
	}
	
	
	public static ElectionManager getInstance(String id, int votes) {
		instance.compareAndSet(null, new ElectionManager(id, votes));
	//	System.out.println("inside election manager get instace"+instance);
		return instance.get();
	}

	public static ElectionManager getInstance() {
		return instance.get();
	}

	/**
	 * initialize the manager for this server
	 * 
	 * @param nodeId
	 *            The server's (this) ID
	 */
	protected ElectionManager(String nodeId, int votes) {
		this.nodeId = nodeId;

		if (votes >= 0)
			this.votes = votes;
	}

	public void setServerConfig(ServerConf conf) {
		this.conf = conf;
	}

	/**
	 * @param args
	 */
	public void processRequest(LeaderElection req) {
		
		nodeId= currentNode;
		if (req == null)
		{
			System.out.println("request is null so returning");
			return;
		}
		if (req.hasExpires()) {
			long ct = System.currentTimeMillis();
			if (ct > req.getExpires()) {
				// election is over
				System.out.println("request has expired so return");
				return;
			}
		}
		if (req.getVote().getNumber() == VoteAction.ELECTION_VALUE) 
		{
			logger.info("Election declared message received");
			IsLeaderElectionStarted = true;
			HeartbeatListener.ifNodeCrashed=false;

			if (nodeId.equalsIgnoreCase(req.getNodeId())) 
				{
					logger.info(" *****************Nominating myself******************");
					LeaderElection.Builder leBuilder = LeaderElection.newBuilder();
					leBuilder.setBallotId("1234");
					leBuilder.setDesc("Nominate");
					leBuilder.setNodeId(nodeId);
					leBuilder.setVote(LeaderElection.VoteAction.NOMINATE);
					leBuilder.setExpires(System.currentTimeMillis() + 3 * 1000);
	
					Management.Builder m = Management.newBuilder();
					m.setElection(leBuilder);	
					sendToAdjacentNodes(m.build());
			} 
			else
			{
				LeaderElection.Builder leBuilder = LeaderElection.newBuilder();
				leBuilder.setBallotId("123");
				leBuilder.setDesc("Election declared");
				leBuilder.setNodeId(req.getNodeId());
				leBuilder.setVote(LeaderElection.VoteAction.ELECTION);
				leBuilder.setExpires(System.currentTimeMillis() + 3 * 1000);

				Management.Builder mb = Management.newBuilder();
				mb.setElection(leBuilder);
				sendToAdjacentNodes(mb.build());			
			}
		} 
		else if (req.getVote().getNumber() == VoteAction.DECLAREVOID_VALUE) 
		{
				}
		else if (req.getVote().getNumber() == VoteAction.DECLAREWINNER_VALUE) 
		{
			isLeaderElected = true;
			HeartbeatListener.ifNodeCrashed=false;
			if(!req.getNodeId().equalsIgnoreCase(nodeId)){
				logger.info("************" + req.getNodeId() + " *********************  is the winner");
							LeaderElection.Builder leBuilder = LeaderElection.newBuilder();
				leBuilder.setBallotId("123");
				leBuilder.setDesc("forwarding	");
				leBuilder.setNodeId(req.getNodeId());
				leBuilder.setVote(LeaderElection.VoteAction.DECLAREWINNER);
				
				LeaderNode=req.getNodeId();
				Management.Builder mb = Management.newBuilder();
				mb.setElection(leBuilder);
				sendToAdjacentNodes(mb.build());
			}
			
		} else if (req.getVote().getNumber() == VoteAction.ABSTAIN_VALUE) {
			// for some reason, I decline to vote
		} else if (req.getVote().getNumber() == VoteAction.NOMINATE_VALUE) {
			logger.info("Node has nominated");
			if (req.getNodeId().equalsIgnoreCase(nodeId)) {
			}

			int comparedToMe = req.getNodeId().compareTo(nodeId);
			logger.info("*************comparing***********"
					+ req.getNodeId() + " with my node Id" + nodeId);
			
			if (comparedToMe > 0) {
				logger.info(" *****************Higher priority node id forwad*******************");
		
				LeaderElection.Builder leBuilder = LeaderElection.newBuilder();
				leBuilder.setBallotId("123");
				leBuilder.setDesc("forwarding	");
				leBuilder.setNodeId(req.getNodeId());
				leBuilder.setVote(LeaderElection.VoteAction.NOMINATE);

				Management.Builder mb = Management.newBuilder();
				mb.setElection(leBuilder);
				sendToAdjacentNodes(mb.build());

			} else if (comparedToMe < 0) {
							logger.info("*******************Forwarding my node id*******************");

				LeaderElection.Builder leBuilder = LeaderElection.newBuilder();
				leBuilder.setBallotId("123");
				leBuilder.setDesc("forwarding	");
				leBuilder.setNodeId(nodeId);
				leBuilder.setVote(LeaderElection.VoteAction.NOMINATE);

				Management.Builder mb = Management.newBuilder();
				mb.setElection(leBuilder);

				sendToAdjacentNodes(mb.build());

			} else {
				isLeaderElected = true;
				HeartbeatListener.ifNodeCrashed=false;
	
				LeaderElection.Builder leBuilder = LeaderElection.newBuilder();
				leBuilder.setBallotId("123");
				leBuilder.setDesc("winner declared");
				leBuilder.setNodeId(nodeId);
				leBuilder.setVote(LeaderElection.VoteAction.DECLAREWINNER);
				LeaderNode=req.getNodeId();
				Management.Builder mb = Management.newBuilder();
				mb.setElection(leBuilder);
				logger.info("****************We got a winneer*****************");
				
				sendToAdjacentNodes(mb.build());

			}
		}
	}

	public void initiateElection() {
		if (!isLeaderElected) {
		logger.info("**************** In initiate election **************8");	
			LeaderElection.Builder leBuilder = LeaderElection.newBuilder();
			leBuilder.setBallotId("123");
			leBuilder.setDesc("i *********** starting the leader election **************");
			leBuilder.setNodeId(nodeId);
			leBuilder.setVote(LeaderElection.VoteAction.NOMINATE);
			leBuilder.setExpires(System.currentTimeMillis() + 3 * 1000);

			Management.Builder mb = Management.newBuilder();
			mb.setElection(leBuilder);
			// send message to all the neigbours
			sendToAdjacentNodes(mb.build());

		}

	}

	/**
	 * Send a message to all the neighboring nodes
	 * 
	 * @param msg
	 */
	public void sendToAdjacentNodes(GeneratedMessage msg) {

		logger.info("******************* In adjacent nodes : leader election *******************");

		for (NodeDesc nn : conf.getNearest().getNearestNodes().values()) {

			LeaderElectionMonitor leMonitor = new LeaderElectionMonitor(nodeId, nn.getHost(),
					nn.getMgmtPort());
			leMonitor.addListener(new HeartPrintListener(null));
			leMonitor.sendMessage(msg);
			leMonitor.release();
		}

	}
	
	public static String getLeaderNodeId() {
		
		return LeaderNode;
		}

}