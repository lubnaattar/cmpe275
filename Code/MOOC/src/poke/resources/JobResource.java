/*
 * copyright 2012, gash
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
package poke.resources;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import poke.server.resources.Resource;
import poke.server.resources.ResourceFactory;
import poke.server.resources.ResourceUtil;
import eye.Comm.*;
import eye.Comm.NameValueSet.NodeType;

//import poke.server.storage.MongoDataAccessObject;
import com.mongodb.*;

public class JobResource implements Resource {
	protected static Logger logger = LoggerFactory.getLogger("server");
	
	
	@Override
    public Request process(Request request) {
	 
		logger.info(" Signup: " + request.getBody().getSignUp());

		Request.Builder rb = Request.newBuilder();
		
		 try  {
					  	 
			    MongoClient mongo=new MongoClient("localhost",27017);
				DB db=mongo.getDB("cmpe275");
				
				
				if(request.getBody().hasSignUp()){
			// Starting of Sign up		 
					try {
				// Extracting data from the body of request
					String name=request.getBody().getSignUp().getFullName();
					String uname=request.getBody().getSignUp().getUserName();
					String pass=request.getBody().getSignUp().getPassword();
					
				// Insertion of document	
				 DBCollection d=db.getCollection("users");
				 BasicDBObject bo=new BasicDBObject("name",name).append("uname", uname).append("pass",pass);
				 d.insert(bo);
				
				// Header Creation
			
				logger.info("S");

				// metadata
				rb.setHeader(ResourceUtil.buildHeaderFrom(request.getHeader(), PokeStatus.SUCCESS,"User Signed up Successfully"));

				// payload
				 Payload.Builder pb = Payload.newBuilder();
			     rb.setBody(pb.build());
                 Request reply = rb.build();
                 logger.info(Integer.toString((reply.getHeader().getRoutingId().getNumber())));
                 return reply;
					}
			           
					catch (Exception e)
					{
						rb.setHeader(ResourceUtil.buildHeaderFrom(request.getHeader(), PokeStatus.FAILURE,"Failed to sign up "));
						
						// payload when unable to sign up
						 Payload.Builder pb = Payload.newBuilder();
					     rb.setBody(pb.build());
		                 Request reply = rb.build();
		                 logger.info(Integer.toString((reply.getHeader().getRoutingId().getNumber())));
		                 return reply;
					}
				// Ending of Sign up	
				}
				if(request.getBody().hasReqList())
				{    
					   RequestList.Builder rl= RequestList.newBuilder();
					   
					
					// Request method to generate 
					logger.info("inside req list");
					
					    System.out.println("hi");
					try {
						DBCollection  r= db.getCollection("course");
					    DBCursor cr=r.find();
						 while(cr.hasNext())
							  {
							  BasicDBObject doc= (BasicDBObject)cr.next();
						    GetCourse.Builder gc=  GetCourse.newBuilder();
							 System.out.println(doc.getInt("courseid")+doc.getString("coursename")+doc.getString("course_description"));
							 gc.setCourseId(doc.getInt("courseid")) ;
							  gc.setCourseName(doc.getString("coursename"));
							  gc.setCourseDescription( doc.getString("course_description") );
							  rl.addCourseList(gc);
							 }
						// Header Creation
					
						

						// metadata
						rb.setHeader(ResourceUtil.buildHeaderFrom(request.getHeader(), PokeStatus.SUCCESS,"RequestList"));

						// payload
						 Payload.Builder pb = Payload.newBuilder();
					     pb.setReqList(rl.build());
					     rb.setBody(pb.build());
		                 Request reply = rb.build();
		                 logger.info(Integer.toString((reply.getHeader().getRoutingId().getNumber())));
		                 return reply;
							}
					           
							catch (Exception e)
							{
								rb.setHeader(ResourceUtil.buildHeaderFrom(request.getHeader(), PokeStatus.FAILURE,"Failed to sign up "));
								
								// payload when unable to sign up
								 Payload.Builder pb = Payload.newBuilder();
							     rb.setBody(pb.build());
				                 Request reply = rb.build();
				                 logger.info(Integer.toString((reply.getHeader().getRoutingId().getNumber())));
				                 return reply;
							}
					
					//  End of request list */
				}

		 }
		 catch(Exception e )
		 {
			 
			 System.out.print("exception in connection");
		 }
		 logger.info("insided the outer loop");
		 Payload.Builder pb = Payload.newBuilder();
		 Ping.Builder fb = Ping.newBuilder();
		 fb.setTag(request.getBody().getPing().getTag());
		 fb.setNumber(request.getBody().getPing().getNumber());
		 pb.setPing(fb.build());
		 rb.setBody(pb.build());

		Request reply = rb.build();

		return reply;
	}

	public Management processMgmtRequest(Request request) {
		// TODO Auto-generated method stub
		//the request received is a job serving request
		
		logger.info("Creating a job processing request ..! "+request.getBody().getJobOp().getData().getJobId());

		String nodeId = ResourceFactory.getInstance().getCfg().getServer().getProperty("node.id");
		Request.Builder rb = Request.newBuilder();
		// metadata
		rb.setHeader(ResourceUtil.buildHeaderFrom(request.getHeader(), PokeStatus.SUCCESS, null));

		// payload
		Management.Builder b = Management.newBuilder();
		JobProposal.Builder jp = JobProposal.newBuilder();
		
		jp.setJobId(request.getBody().getJobOp().getData().getJobId());
		jp.setOwnerId(Integer.parseInt(ResourceFactory.getInstance().getCfg().getServer().getProperty("node.id")));
		
		//setting the weight of the proposal - how heavy is the request
		int min = 1, max = 10;
		int weight = min + (int)(Math.random() * ((max - min) + 1));
		jp.setWeight(weight);
		
		NameValueSet.Builder nameVal = NameValueSet.newBuilder();
		nameVal.setNodeType(NodeType.NODE);
		nameVal.setName(nodeId);
		jp.setOptions(nameVal);
		
		if(request.getBody().getJobOp().getData().hasNameSpace())
			jp.setNameSpace(request.getBody().getJobOp().getData().getNameSpace());
		else
			jp.setNameSpace("JobProposal");
		
		b.setJobPropose(jp);
		Management reply = b.build();
		return reply;
	}

}


