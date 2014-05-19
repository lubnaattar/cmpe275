package poke.demo;

import poke.client.ClientCommand;
import poke.client.ClientPrintListener;
import poke.client.comm.CommListener;

public class reqlist {
	
	

	public void run() {
		ClientCommand cc = new ClientCommand("localhost", 5570);
		CommListener listener = new ClientPrintListener("reqlist");
		cc.addListener(listener);
            cc.reqlist();
	}

	public static void main(String[] args) {
		try {
			reqlist sign  = new reqlist();
			   sign.run();
			// we are running asynchronously
			System.out.println("\nExiting in 5 seconds");
			

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
