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

import io.netty.channel.Channel;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eye.Comm.Network;
import eye.Comm.Network.NetworkAction;

/**
 * The network manager contains the node's view of the network.
 * 
 * @author gash
 * 
 */
public class NetworkManager {
	protected static Logger logger = LoggerFactory.getLogger("management");
	protected static AtomicReference<NetworkManager> instance = new AtomicReference<NetworkManager>();

	private String nodeId;

	/** @brief the number of votes this server can cast */
	private int votes = 1;

	public static NetworkManager getInstance(String id) {
		instance.compareAndSet(null, new NetworkManager(id)); //argumets are (expect,update) automatically sets the  value to given updated value if current value ==expected value
		return instance.get();
	}

	public static NetworkManager getInstance() {
		return instance.get();
	}

	/**
	 * initialize the manager for this server
	 * 
	 * @param nodeId
	 *            The server's (this) ID
	 */
	protected NetworkManager(String nodeId) {
		this.nodeId = nodeId;
	}

	/**
	 * @param args
	 */
	public void processRequest(Network req, Channel channel, SocketAddress sa) {
		if (req == null || channel == null || sa == null)
		{
			logger.info("req is"+req+"channel is"+channel+"socket address is"+sa);
			System.out.println("kuch toh null aara hai bhai");
			return;
			
		}
logger.info("req is"+req+"channel is"+channel+"socket address is"+sa);
		logger.info("Network: node '" + req.getNodeId() + "' sent a " + req.getAction());

		/**
		 * Outgoing: when a node joins to another node, the connection is
		 * monitored to relay to the requester that the node (this) is active -
		 * send a heartbeatMgr
		 */
		if (req.getAction().getNumber() == NetworkAction.NODEJOIN_VALUE) {
			if (channel.isOpen()) {
				// can i cast socka?
				SocketAddress socka = channel.localAddress();
				if (socka != null) {
					InetSocketAddress isa = (InetSocketAddress) socka;
					logger.info("NODEJOIN: " + isa.getHostName() + ", " + isa.getPort());
					HeartbeatManager.getInstance().addOutgoingChannel(req.getNodeId(), isa.getHostName(),
							isa.getPort(), channel, sa);
				}
			} else
				logger.warn(req.getNodeId() + " not writable");
		} else if (req.getAction().getNumber() == NetworkAction.NODEDEAD_VALUE) 
		{
			if (channel.isOpen()) {
				// can i cast socka?
				
					channel.disconnect();
				}
			logger.info("possible failure - node is considered dead....disconnecting chanel");
		} 
		else if (req.getAction().getNumber() == NetworkAction.NODELEAVE_VALUE) 
		{
			/*if (channel.isOpen()) {
				// can i cast socka?
				SocketAddress socka = channel.localAddress();
				if (socka != null) {
					InetSocketAddress isa = (InetSocketAddress) socka;
					logger.info("NODELEAVE: " + isa.getHostName() + ", " + isa.getPort());
					HeartbeatManager.getInstance().removeAdjacentNode();
					
					//addOutgoingChannel(req.getNodeId(), isa.getHostName(),
							isa.getPort(), channel, sa);
			logger.info("node removing itself from the network (gracefully)");*/
			
		}
		else if (req.getAction().getNumber() == NetworkAction.ANNOUNCE_VALUE) 
		{
			// 
			logger.info("nodes sending their info in response to a create map");
		}
		else if (req.getAction().getNumber() == NetworkAction.CREATEMAP_VALUE) 
		{
			// 
			logger.info("request to create a network topology map");
		}

		// may want to reply to exchange information
	}
}
