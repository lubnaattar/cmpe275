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
package poke.server.queue;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;

import java.lang.Thread.State;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingDeque;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import poke.server.JobOpManager;
import poke.server.management.managers.ElectionManager;
import poke.server.management.managers.JobManager;
import poke.server.resources.Resource;
import poke.server.resources.ResourceFactory;
import poke.server.resources.ResourceUtil;
import poke.server.storage.jdbc.DatabaseStorage;

import com.google.protobuf.GeneratedMessage;

import eye.Comm.Header;
import eye.Comm.JobBid;
import eye.Comm.JobDesc;
import eye.Comm.JobOperation;
import eye.Comm.JobStatus;
import eye.Comm.Management;
import eye.Comm.Payload;
import eye.Comm.PokeStatus;
import eye.Comm.Request;
import eye.Comm.Header.Routing;
import eye.Comm.JobDesc.JobCode;

/**
 * A server queue exists for each connection (channel). A per-channel queue
 * isolates clients. However, with a per-client model. The server is required to
 * use a master scheduler/coordinator to span all queues to enact a QoS policy.
 * 
 * How well does the per-channel work when we think about a case where 1000+
 * connections?
 * 
 * @author gash
 * 
 */
public class PerChannelQueue implements ChannelQueue {
	protected static Logger logger = LoggerFactory.getLogger("server");

	private Channel channel;

	// The queues feed work to the inbound and outbound threads (workers). The
	// threads perform a blocking 'get' on the queue until a new event/task is
	// enqueued. This design prevents a wasteful 'spin-lock' design for the
	// threads
	private LinkedBlockingDeque<com.google.protobuf.GeneratedMessage> inbound;
	private LinkedBlockingDeque<com.google.protobuf.GeneratedMessage> outbound;
	private Queue<JobBid> bidResponse = new LinkedBlockingDeque<JobBid>();

	// This implementation uses a fixed number of threads per channel
	private OutboundWorker oworker;
	private InboundWorker iworker;

	// not the best method to ensure uniqueness
	private ThreadGroup tgroup = new ThreadGroup("ServerQueue-" + System.nanoTime());
	DatabaseStorage storage = new DatabaseStorage();

	
	protected PerChannelQueue(Channel channel) {
		this.channel = channel;
		init();
	}
	public Integer NodeIdToInt(String nodeId) {
		Integer i_id = 0;
		if (nodeId.equals("zero")) {
			i_id = 0;
		} else if (nodeId.equals("one")) {
			i_id = 1;
		} else if (nodeId.equals("two")) {
			i_id = 2;
		} else if (nodeId.equals("three")) {
			i_id = 3;
		}
		return i_id;
	}

	public String IntToNodeId(Integer i_Id) {
		String nodeId = "";
		switch (i_Id) {
		case 0:
			nodeId = "zero";
			break;
		case 1:
			nodeId = "one";
			break;
		case 2:
			nodeId = "two";
			break;
		case 3:
			nodeId = "three";
			break;
		}
		return nodeId;
	}


	protected void init() {
		inbound = new LinkedBlockingDeque<com.google.protobuf.GeneratedMessage>();
		outbound = new LinkedBlockingDeque<com.google.protobuf.GeneratedMessage>();

		iworker = new InboundWorker(tgroup, 1, this);
		iworker.start();

		oworker = new OutboundWorker(tgroup, 1, this);
		oworker.start();

	}

	protected Channel getChannel() {
		return channel;
	}
	public void putBidResponse(JobBid bidReq) {
		bidResponse.add(bidReq);
	}


	/*
	 * (non-Javadoc)
	 * 
	 * @see poke.server.ChannelQueue#shutdown(boolean)
	 */
	@Override
	public void shutdown(boolean hard) {
		logger.info("server is shutting down");

		channel = null;

		if (hard) {
			// drain queues, don't allow graceful completion
			inbound.clear();
			outbound.clear();
		}

		if (iworker != null) {
			iworker.forever = false;
			if (iworker.getState() == State.BLOCKED || iworker.getState() == State.WAITING)
				iworker.interrupt();
			iworker = null;
		}

		if (oworker != null) {
			oworker.forever = false;
			if (oworker.getState() == State.BLOCKED || oworker.getState() == State.WAITING)
				oworker.interrupt();
			oworker = null;
		}

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see poke.server.ChannelQueue#enqueueRequest(eye.Comm.Finger)
	 */
	@Override
	public void enqueueRequest(Request req, Channel notused) {
		try {
			inbound.put(req);
		} catch (InterruptedException e) {
			logger.error("message not enqueued for processing", e);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see poke.server.ChannelQueue#enqueueResponse(eye.Comm.Response)
	 */
	@Override
	public void enqueueResponse(Request reply, Channel notused) {
		if (reply == null)
			return;

		try {
			outbound.put(reply);
		} catch (InterruptedException e) {
			logger.error("message not enqueued for reply", e);
		}
	}

	protected class OutboundWorker extends Thread {
		int workerId;
		PerChannelQueue sq;
		boolean forever = true;

		public OutboundWorker(ThreadGroup tgrp, int workerId, PerChannelQueue sq) {
			super(tgrp, "outbound-" + workerId);
			this.workerId = workerId;
			this.sq = sq;

			if (outbound == null)
				throw new RuntimeException("connection worker detected null queue");
		}

		@Override
		public void run() {
			Channel conn = sq.channel;
			if (conn == null || !conn.isOpen()) {
				PerChannelQueue.logger.error("connection missing, no outbound communication");
				return;
			}
			while (true) {
				if (!forever && sq.outbound.size() == 0)
					break;
				try {
					// block until a message is enqueued
					GeneratedMessage msg = sq.outbound.take();
					if (conn.isWritable()) {
						boolean rtn = false;
						if (channel != null && channel.isOpen() && channel.isWritable()) {
							ChannelFuture cf = channel.writeAndFlush(msg);
							cf.awaitUninterruptibly();
							rtn = cf.isSuccess();
							if (!rtn)
								sq.outbound.putFirst(msg);
						}
					} 
					else
						sq.outbound.putFirst(msg);
				} catch (InterruptedException ie) {
					break;
				} catch (Exception e) {
					PerChannelQueue.logger.error("Unexpected communcation failure", e);
					break;
				}
			}
			if (!forever) {
				PerChannelQueue.logger.info("connection queue closing");
			}
		}
	}

	protected class InboundWorker extends Thread {
		int workerId;
		PerChannelQueue sq;
		boolean forever = true;

		// variable to store the jobOperation request
		Request reqOperation;
		DatabaseStorage storage = new DatabaseStorage();

		public InboundWorker(ThreadGroup tgrp, int workerId, PerChannelQueue sq) {
			super(tgrp, "inbound-" + workerId);
			this.workerId = workerId;
			this.sq = sq;

			if (outbound == null)
				throw new RuntimeException(
						"connection worker detected null queue");
		}

		public boolean isLeader() {
			return ElectionManager.getInstance().getLeaderNodeId()
					.equals(getMyNode());
		}

		public String getMyNode() {
			return ResourceFactory.getInstance().getCfg().getServer()
					.getProperty("node.id");
		}

		@Override
		public void run() {
			Channel conn = sq.channel;
			logger.info("PerChannel InbondWorker started");
			if (conn == null || !conn.isOpen()) {
				PerChannelQueue.logger
						.error("connection missing, no inbound communication");
				return;
			}

			while (true) {
				if (!forever && sq.inbound.size() == 0)
					break;
				try {
					// block until a message is enqueued
					GeneratedMessage msg = sq.inbound.take();
					logger.info("PerChannel InbondWorker Got a message....");
					// process request and enqueue response
					if (msg instanceof Request) {
						Request req = ((Request) msg);
						Resource rsc = ResourceFactory.getInstance()
								.resourceInstance(req.getHeader());
						Request reply = null;
						if (rsc == null) {
							logger.error("failed to obtain resource for " + req);
							reply = ResourceUtil.buildError(req.getHeader(),
									PokeStatus.NORESOURCE,
									"Request not processed");
						}
						// if the request is for serving a job - pass the
						// request to job manager as jobProposal
						if (req.getHeader().getRoutingId().getNumber() == Routing.JOBS
								.getNumber()) {
							// if the request is for JobOperation, process it
							if (req.getBody().hasJobOp()) {
								if (isLeader()) {
									logger.info("Received a JobOp request .. I am the leader.. Forwarding the request");
									reqOperation = req;
									Management mgmt = rsc
											.processMgmtRequest(req);
									addJobToQueue(mgmt);
									JobBid bidReq = waitForBid();
									Request result = createJobOperation(bidReq);
									JobOpManager.getInstance()
											.submitJobOperation(sq, result);
								} else {
									logger.info("Received a JobOperation request . Checking if the request is for me.");
									// Check if I have to handle the request
									if (req.getHeader().getToNode()
											.equals(getMyNode())) {
										logger.info("My bidding was highest.. This JobOp request is for me.. Processing it..");
										// if I get a job operation with ADD JOB
										// code, then add to DB
										if (req.getBody().getJobOp()
												.getAction().getNumber() == 1) {
											try {
												// access the database, do the
												// operation
												// sending the job desc object
												// to the DB
												Boolean b = storage.addJob(req
														.getBody().getJobOp()
														.getData()
														.getNameSpace(), req
														.getBody().getJobOp()
														.getData());
												if (b)
													logger.info("Job desc added to the DB..");
												else
													logger.info("Job desc not added to the DB");

												logger.info("Job persisted.. Creating status response for client");
												// create job status request
												Request status = createJobStatus(
														req, b, null);

												logger.info("Created status request for Job Operation.. ");
												// send the Job Status request
												// back to the client
												JobOpManager
														.getInstance()
														.submitJobStatus(status);
												logger.info("Forwarding the Status Request");

											} catch (Exception e) {
												logger.info("Exception encountered in persisiting to the DB : "
														+ e);
											}
										}
										// if I get a job operation with REMOVE
										// JOB code, then readdmove then job
										// from the DB
										else if (req.getBody().getJobOp()
												.getAction().getNumber() == 3) {
											try {
												Boolean b = storage
														.removeJob(
																req.getBody()
																		.getJobOp()
																		.getData()
																		.getNameSpace(),
																req.getBody()
																		.getJobOp()
																		.getJobId());
												if (b)
													logger.info("Job desc removed from the DB..");
												else
													logger.info("Could not remove job desc from the DB");

												// create job status request
												Request status = createJobStatus(
														req, b, null);

												// send the Job Status request
												// back to the client
												JobOpManager
														.getInstance()
														.submitJobStatus(status);
											} catch (Exception e) {
												logger.info("Exception encountered in removing entries from the DB : "
														+ e);
											}
										}
										// if the job operation request if for
										// finding the jobs
										else if (req.getBody().getJobOp()
												.getAction().getNumber() == 4) {
											try {
												Boolean b = false;
												List<JobDesc> listJobs = storage
														.findJobs(
																req.getBody()
																		.getJobOp()
																		.getData()
																		.getNameSpace(),
																req.getBody()
																		.getJobOp()
																		.getData());
												if (!listJobs.isEmpty()) {
													b = true;
													logger.info("Fetched job lists from the DB for JObID : "
															+ req.getBody()
																	.getJobOp()
																	.getJobId());
												} else
													logger.info("No Jobs found with the JobID : "
															+ req.getBody()
																	.getJobOp()
																	.getJobId());

												// create job status request
												Request status = createJobStatus(
														req, b, listJobs);

												// send the Job Status request
												// back to the client
												JobOpManager
														.getInstance()
														.submitJobStatus(status);
											} catch (Exception e) {
												logger.info("Exception encountered in finding data from the DB : "
														+ e);
											}
										}
									} else {
										logger.info("I do not have to serve this JobOp request. Forwarding it..");
										// Forward the Job for other node
										// tohandle
										JobOpManager.getInstance()
												.sendResponse(req);
									}
								}
							}
							// if the request has a JobStatus, the request has
							// been processed, it needs to be sent back to the
							// client
							else if (req.getBody().hasJobStatus()) {
								if (isLeader()) {
									logger.info("Received a JobStatus request.. I am the leader.. Sending it to the client");
									// add to outbound queue, write to client
									JobOpManager.getInstance().submitJobStatus(
											req);
								} else {
									logger.info("Received a JobStatus request.. Forwarding it till it reaches the leader..");
									// send to next node
									JobOpManager.getInstance().submitJobStatus(
											req);

								}
							}
							// else {
							// // handle it locally
							// logger.info("Processing ping requests here..");
							// logger.info("Request received - ", req);
							// logger.info("Reply to be sent - ", reply);
							// reply = rsc.process(req);
							// sq.enqueueResponse(reply, null);
							// }
						}

						else {
							// handle it locally
							logger.info("Processing ping requests here..");
							logger.info("Request received - ", req.toString());
							reply = rsc.process(req);
							logger.info("Reply to be sent - ", reply.toString());
							sq.enqueueResponse(reply, null);
						}

					}
				} catch (InterruptedException ie) {
					break;
				} catch (Exception e) {
					PerChannelQueue.logger.error(
							"Unexpected processing failure", e);
					break;
				}
			}

			if (!forever) {
				PerChannelQueue.logger.info("connection queue closing");
			}
		}

		// method to place a request of job processing on Job Manager
		public void addJobToQueue(Management jobReq) {
			logger.info("Job Bid at PCQ added to queue");
			JobManager.getInstance().submitJobProposal(sq, jobReq);
		}

		public JobBid waitForBid() {
			while (bidResponse.isEmpty()) {
				try {
					this.sleep(200);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			logger.info("PCQ recieved a response for Job Proposal");
			return bidResponse.remove();
		}

		/**
		 * If the request has been processed by the cluster/any client : forward
		 * the Job Status request
		 * 
		 * @param jobstatusreq
		 * @return
		 */
		public void submitJobStatus(Request jobstatusreq) {
			logger.info("Pushing the request to the client channel..! In submitJobStatus()");
			sq.enqueueResponse(jobstatusreq, null);
		}

		/**
		 * If the Job has been processed by the cluster : create a Job Status
		 * request
		 * 
		 * @param jobOp
		 * @return
		 */
		public Request createJobStatus(Request jobOp, boolean b,
				List<JobDesc> jobDescList) {
			try {
				logger.info("Request is: \n", jobOp.toString());
				logger.info("Inside createJobStatus() ..");
				JobStatus.Builder js = JobStatus.newBuilder();
				js.setJobId(jobOp.getBody().getJobOp().getData().getJobId());
				if (b)
					js.setStatus(PokeStatus.SUCCESS);
				else
					js.setStatus(PokeStatus.FAILURE);

				js.setJobState(JobCode.JOBRECEIVED);

				// if the List returned is not null ,then associate it with the
				// JobStatus
				if (jobDescList != null) {
					if (!jobDescList.isEmpty()) {
						logger.info("List of Job Desc retireved from DB is not Empty.. !!");
						for (int i = 0; i < jobDescList.size(); i++)
							js.setData(i, jobDescList.get(i));
					}
				}

				/*else {
					JobDesc.Builder jdesc = JobDesc.newBuilder();
					jdesc.setNameSpace(jobOp.getBody().getJobOp().getData()
							.getNameSpace());
					jdesc.setOwnerId((jobOp.getBody().getJobOp().getData()
							.getOwnerId()));
					jdesc.setJobId((jobOp.getBody().getJobOp().getData()
							.getJobId()));
					jdesc.setStatus(JobCode.JOBRECEIVED);
					js.setData(0, jdesc);
				}*/
				// payload containing data for job
				Request.Builder r = Request.newBuilder();
				eye.Comm.Payload.Builder p = Payload.newBuilder();
				p.setJobStatus(js.build());
				r.setBody(p.build());

				// header with routing info
				Header.Builder h = Header.newBuilder();
				h.setOriginator(getMyNode());
				h.setTag(jobOp.getHeader().getTag());
				h.setTime(jobOp.getHeader().getTime());
				h.setRoutingId(jobOp.getHeader().getRoutingId());
				h.setToNode("client");

				r.setHeader(h.build());
				Request req = r.build();
				logger.info("New Job reply status request formed.. Sending it back to the client");
				return req;
			} catch (Exception npe) {
				npe.printStackTrace();
			}
			return null;

		}

		public Request createJobOperation(JobBid bidReq) {

			int recNode = (int) (bidReq.getOwnerId());
			String sendReqtoNode = IntToNodeId(recNode);
			String myNodeId = ResourceFactory.getInstance().getCfg()
					.getServer().getProperty("node.id");
			logger.info("Adding the bid to my own queue and forwarding the job proposal to the respective node");
			logger.info("Bid received for serving the request from the node : "
					+ sendReqtoNode);

			// modifying the joboperation header and payload - sending the
			// message to the node which has won the bid
			Request jobOpRequest = reqOperation;

			JobOperation.Builder j = JobOperation.newBuilder();
			j.setAction(jobOpRequest.getBody().getJobOp().getAction());
			j.setJobId(jobOpRequest.getBody().getJobOp().getJobId());
			// j.setData(jobOpRequest.getBody().getJobOp().getData());

			JobDesc.Builder desc = JobDesc.newBuilder();
			desc.setNameSpace(jobOpRequest.getBody().getJobOp().getData()
					.getNameSpace());
			desc.setOwnerId(NodeIdToInt(myNodeId));
			desc.setJobId(jobOpRequest.getBody().getJobOp().getData()
					.getJobId());
			desc.setStatus(jobOpRequest.getBody().getJobOp().getData()
					.getStatus());
			// desc.setOptions(value)
			j.setData(desc);

			// payload containing data for job
			Request.Builder r = Request.newBuilder();
			eye.Comm.Payload.Builder p = Payload.newBuilder();
			p.setJobOp(j.build());
			r.setBody(p.build());

			// header with routing info
			Header.Builder h = Header.newBuilder();
			h.setOriginator(myNodeId);
			h.setTag(jobOpRequest.getHeader().getTag());
			h.setTime(jobOpRequest.getHeader().getTime());
			h.setRoutingId(jobOpRequest.getHeader().getRoutingId());
			h.setToNode(sendReqtoNode);

			r.setHeader(h.build());
			Request req = r.build();

			logger.info("New job operation request formed.. Sending it to the DB for persisting..!!");
			return req;
		}

	}

	/**
	 * If a response is received after bidding is done, forward the job
	 * operation request to the server which has sent the bid
	 * 
	 * @param bidReq
	 */
	

	public class WriteListener implements ChannelFutureListener {
		private ChannelQueue sq;

		public WriteListener(ChannelQueue sq) {
			this.sq = sq;
		}

		@Override
		public void operationComplete(ChannelFuture future) throws Exception {
			logger.info("Write complete");
			if (future.isSuccess() != true) {
				logger.info("isSuccess: " + future.isSuccess());
				logger.info("Cause: " + future.cause());
			}
			// sq.shutdown(true);
		}
	}


	public class CloseListener implements ChannelFutureListener {
		private ChannelQueue sq;
		public CloseListener(ChannelQueue sq) 
		{
			this.sq = sq;
		}
		@Override
		public void operationComplete(ChannelFuture future) throws Exception {
			sq.shutdown(true);
		}
	}
}
