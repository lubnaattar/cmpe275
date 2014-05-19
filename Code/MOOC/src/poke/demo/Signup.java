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
package poke.demo;

import poke.client.ClientCommand;
import poke.client.ClientPrintListener;
import poke.client.comm.CommListener;

/**
 * DEMO: how to use the command class
 * 
 * @author gash
 * 
 */
public class Signup {
	private String n;
	private String u;
	private String p;
	

	public Signup(String name, String uname, String pass) {
		this.n = name;
		this.u=uname;
		this.p=pass;
		
	}

	public void run() {
		ClientCommand cc = new ClientCommand("localhost", 5570);
		CommListener listener = new ClientPrintListener("jab demo");
		cc.addListener(listener);
            cc.signup(n,u,p);
	}

	public static void main(String[] args) {
		try {
			Signup sign  = new Signup("Demo response", "viki","get dumb");
			sign.run();

			// we are running asynchronously
			System.out.println("\nExiting in 5 seconds");
			

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}

