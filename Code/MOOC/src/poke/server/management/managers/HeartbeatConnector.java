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

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import poke.monitor.HeartMonitor;
import poke.monitor.MonitorHandler;
import poke.server.management.managers.HeartbeatData.BeatStatus;

/**
 * The connector collects connection monitors (e.g., listeners implement the
 * circuit breaker) that maintain HB communication between nodes (to
 * client/requester).
 * 
 * @author gash
 * 
 */
public class HeartbeatConnector extends Thread {
	protected static Logger logger = LoggerFactory.getLogger("management");
	protected static AtomicReference<HeartbeatConnector> instance = new AtomicReference<HeartbeatConnector>();
	

	private ConcurrentLinkedQueue<HeartMonitor> monitors = new ConcurrentLinkedQueue<HeartMonitor>();
	private int sConnectRate = 2000; // msec
	private boolean forever = true;

	public static HeartbeatConnector getInstance() {
		instance.compareAndSet(null, new HeartbeatConnector());
		return instance.get();
	}

	/**
	 * The connector will only add nodes for connections that this node wants to
	 * establish. Outbound (we send HB messages to) requests do not come through
	 * this class.
	 * 
	 * @param node
	 */
	public void addConnectToThisNode(HeartbeatData node) {
		System.out.println("Entering addconnecttothis node in heartbeat connector");
		// null data is not allowed
		if (node == null || node.getNodeId() == null)
			throw new RuntimeException("Null nodes or IDs are not allowed");

		// register the node to the manager that is used to determine if a
		// connection is usable by the public messaging
		// the below statement calls the heartbeat manager and adds the node to in the list of incomming list.
		HeartbeatManager.getInstance().addAdjacentNode(node);

		// this class will monitor this channel/connection and together with the
		// manager, we create the circuit breaker pattern to separate
		// health-status from usage.
		HeartMonitor hm = new HeartMonitor(node.getNodeId(), node.getHost(), node.getMgmtport());
		
		hm.addListener(new HeartbeatListener(node));

		// add monitor to the list of adjacent nodes that we track
		monitors.add(hm);
	}
	
	
	public boolean stopListening(String nodeId,int port)
	{
		System.out.println("********************** in stop listening **************************");
		for(HeartMonitor monitor: monitors )
		{
			System.out.println("whom am i "+monitor.getWhoami()+" monitor getport "+monitor.getPort());
			System.out.println("node id"+nodeId);
			System.out.println("port OOOOOOOOO"+port);
			if(monitor.getWhoami().equals(nodeId) )
			{
				System.out.println("whom am i "+monitor.getWhoami()+" monitor getport "+monitor.getPort());
				monitor.getHandler().removeListner(nodeId);
				monitors.remove(monitor);
				return true;
			}
		}
		
		
		return false;
		
	}
	
	

	@Override
	public void run() {
		if (monitors.size() == 0) {
			logger.info("HB connection monitor not started, no connections to establish");
			return;
		} else
			logger.info("HB connection monitor starting, node has " + monitors.size() + " connections");

		while (forever) {
			try {
				Thread.sleep(sConnectRate);
				System.out.println("Thread name is"+Thread.currentThread().getName());
				// try to establish connections to our nearest nodes
				for (HeartMonitor hb : monitors) {
					if (!hb.isConnected()) {
						try {
						
						 
							logger.info("attempting to connect to node: " + hb.getNodeInfo());
							
							hb.startHeartbeat();
							
							
							
						
						}
						 catch (Exception ie) {
							// do nothing
						}
						
					}
				}
			} catch (InterruptedException e) {
				logger.error("Unexpected HB connector failure", e);
				break;
			}
		}
		logger.info("ending heartbeatMgr connection monitoring thread");
	}

	public boolean validateConnection() {
		System.out.println("inside validate connection");
		// validate connections this node wants to create
		for (HeartbeatData hb : HeartbeatManager.getInstance().incomingHB.values()) 
		{
			
			// receive HB - need to check if the channel is readable
			if (hb.channel == null) 
				{
				
			
					if (hb.getStatus() != BeatStatus.Active || hb.getStatus() != BeatStatus.Weak || hb.getStatus()!= BeatStatus.Init || hb.getStatus()!= BeatStatus.Unknown) 
						{
							hb.setStatus(BeatStatus.Failed);
							hb.setLastFailed(System.currentTimeMillis());
							hb.incrementFailures();
							
							System.out.println("incomming HB status is set to failed");
							return false;
						}
				}
			
			else if (hb.channel.isOpen()) 
				{
				System.out.println("incomming channel is open");
					if (hb.channel.isWritable()) 
					{
						//System.out.println("channel is writable");
						
						if (System.currentTimeMillis() - hb.getLastBeat() >= hb.getBeatInterval()) 
							{
							
						
									hb.incrementFailures();
									hb.setStatus(BeatStatus.Weak);
							//		System.out.println("incomming HB status is set to weak");
							} 
						else
							{
							
								hb.setStatus(BeatStatus.Active);
								hb.setFailures(0);
						//		System.out.println("incomming HB status is set to active");
							}
						
					}
					else
					hb.setStatus(BeatStatus.Weak);
				} 
			else 
				{
					if (hb.getStatus() != BeatStatus.Init) 
						{
								hb.setStatus(BeatStatus.Failed);
								hb.setLastFailed(System.currentTimeMillis());
								hb.incrementFailures();
					//			System.out.println("incomming status is set to failed as beatstatus is not init");
						}
				}
		}

		// validate connections this node wants to create
		for (HeartbeatData hb : HeartbeatManager.getInstance().outgoingHB.values())
		{
			System.out.println("inside for loop outgoingHB");
			// emit HB - need to check if the channel is writable
			if (hb.channel == null) 
			{
				System.out.println("outgoing channe is null");
				if (hb.getStatus() != BeatStatus.Active || hb.getStatus() != BeatStatus.Weak) 
					{
						hb.setStatus(BeatStatus.Failed);
						hb.setLastFailed(System.currentTimeMillis());
						hb.incrementFailures();
			//			System.out.println("Outgoing HB status is set to Failed");
					}
			} 
			else if (hb.channel.isOpen()) 
			{
		//		System.out.println("outgoing channel is open");
				if (hb.channel.isWritable()) 
					{
		//			System.out.println("outgoing channel is writable");
						if (System.currentTimeMillis() - hb.getLastBeat() >= hb.getBeatInterval()) 
						{
							hb.incrementFailures();
							hb.setStatus(BeatStatus.Weak);
		//					System.out.println("Outgoing HB status is set to weak");
						} 
						else 
						{
							hb.setStatus(BeatStatus.Active);
							hb.setFailures(0);
		//					System.out.println("Outgoing HB status is set to Active");
						}
					}
				else
					hb.setStatus(BeatStatus.Weak);
			}
			else 
			{
				if (hb.getStatus() != BeatStatus.Init) 
					{
						hb.setStatus(BeatStatus.Failed);
						hb.setLastFailed(System.currentTimeMillis());
						hb.incrementFailures();
		//				System.out.println("Outgoing HB status is set to failde as Beatstaus is not Init");
					}
			}
		}
		return true;
	}
}
