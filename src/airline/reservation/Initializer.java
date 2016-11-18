package airline.reservation;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

/**
 * @author Piyush, Himat
 * Airline Reservation System
 * Date: 03/30/2016
 * Initializer class is used to initialize the connection between the application and zookeeper.
 * This class also creates all the znodes and the child znodes required in the application. 
 */
public class Initializer {
	
	/**
	 * Class level variables:
	 * Zookeeper object.
	 * CountDownLatch Object.
	 */
	static ZooKeeper zk = null;
	final static CountDownLatch connectionLatch = new CountDownLatch(1);
	
	/**
	 * @param host
	 * @return
	 * @throws IOException
	 * @throws InterruptedException
	 * This method is used to establish connection with the Zookeper server (zkServer) using the host and the port number.
	 * A watched event is created which triggers the connection once the countdown reaches 1.
	 */
	public static ZooKeeper connect(String host) throws IOException,
	InterruptedException {
		zk = new ZooKeeper(host, 2181, new Watcher() {
	public void process(WatchedEvent we) {

		if (we.getState() == KeeperState.SyncConnected) {
			connectionLatch.countDown();
		}
	}
});

connectionLatch.await();
return zk;
}
	/**
	 * @param args
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws KeeperException
	 * Main method calls the connect method, deletes all the znodes and then calls the createZnodes method.
	 */
	public static void main(String args[]) throws IOException, InterruptedException, KeeperException{
		Logger.getRootLogger().setLevel(Level.OFF);
		connect("127.0.0.1:2181");
		zk.delete("/flights", -1);
		zk.delete("/flights/ORD-OKC",-1);
		zk.delete("/flights/EWR-TUL",-1);
		zk.delete("/flights/DFW-SEA",-1);
		zk.delete("/flights/SFO-JFK",-1);
		zk.delete("/flights/DEL-MAA",-1);
		zk.delete("/flights/ORD-OKC/First",-1);
		zk.delete("/flights/ORD-OKC/Business",-1);
		zk.delete("/flights/ORD-OKC/Economy",-1);
		zk.delete("/flights/EWR-TUL/First",-1);
		zk.delete("/flights/EWR-TUL/Business",-1);
		zk.delete("/flights/EWR-TUL/Economy",-1);
		zk.delete("/flights/DFW-SEA/First",-1);
		zk.delete("/flights/DFW-SEA/Business",-1);
		zk.delete("/flights/DFW-SEA/Economy",-1);
		zk.delete("/flights/SFO-JFK/First",-1);
		zk.delete("/flights/SFO-JFK/Business",-1);
		zk.delete("/flights/SFO-JFK/Economy",-1);
		zk.delete("/flights/DEL-MAA/First",-1);
		zk.delete("/flights/DEL-MAA/Business",-1);
		zk.delete("/flights/DEL-MAA/Economy",-1);
		zk.delete("/booking-order",-1);
		zk.delete("/booking-ids", -1);
		createZnodes();
			
	}
	
	/**
	 * This method creates the znodes required for the application. The znodes are created based on the flight route
	 * the flight schedule and the Class of seat in the flight.
	 */
	public static void createZnodes(){
		try {
		    zk.create("/flights", "flights".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		} catch(InterruptedException intrEx) {
		    System.out.println("");
		} catch(KeeperException kpEx) {
		    System.out.println("");
		}
		    try{
		    	zk.create("/flights/ORD-OKC", "90".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		    } catch(InterruptedException intrEx) {
			    System.out.println("");
			} catch(KeeperException kpEx) {
			    System.out.println("");
			}
		    try{
		    zk.create("/flights/EWR-TUL", "90".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		    zk.create("/flights/DFW-SEA", "90".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		    zk.create("/flights/DEL-MAA", "90".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		    zk.create("/flights/SFO-JFK", "90".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		    zk.create("/flights/ORD-OKC/1700", "90".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		    zk.create("/flights/ORD-OKC/0600", "90".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		    zk.create("/flights/EWR-TUL/1700", "90".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		    zk.create("/flights/EWR-TUL/0600", "90".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		    zk.create("/flights/DFW-SEA/1700", "90".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		    zk.create("/flights/SFO-JFK/1700", "90".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		    zk.create("/flights/DEL-MAA/1700", "90".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		    zk.create("/flights/DFW-SEA/0600", "90".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		    zk.create("/flights/SFO-JFK/0600", "90".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		    zk.create("/flights/DEL-MAA/0600", "90".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		    zk.create("/flights/ORD-OKC/1700/First", "30".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		    zk.create("/flights/ORD-OKC/1700/Business", "30".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		    zk.create("/flights/ORD-OKC/1700/Economy", "30".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		    zk.create("/flights/EWR-TUL/1700/First", "30".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		    zk.create("/flights/EWR-TUL/1700/Business", "30".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		    zk.create("/flights/EWR-TUL/1700/Economy", "30".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		    zk.create("/flights/DFW-SEA/1700/First", "30".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		    zk.create("/flights/DFW-SEA/1700/Business", "30".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		    zk.create("/flights/DFW-SEA/1700/Economy", "30".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		    zk.create("/flights/SFO-JFK/1700/First", "30".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		    zk.create("/flights/SFO-JFK/1700/Business", "30".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		    zk.create("/flights/SFO-JFK/1700/Economy", "30".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		    zk.create("/flights/DEL-MAA/1700/First", "30".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		    zk.create("/flights/DEL-MAA/1700/Business", "30".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		    zk.create("/flights/DEL-MAA/1700/Economy", "30".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		    zk.create("/flights/ORD-OKC/0600/First", "30".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		    zk.create("/flights/ORD-OKC/0600/Business", "30".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		    zk.create("/flights/ORD-OKC/0600/Economy", "30".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		    zk.create("/flights/EWR-TUL/0600/First", "30".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		    zk.create("/flights/EWR-TUL/0600/Business", "30".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		    zk.create("/flights/EWR-TUL/0600/Economy", "30".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		    zk.create("/flights/DFW-SEA/0600/First", "30".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		    zk.create("/flights/DFW-SEA/0600/Business", "30".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		    zk.create("/flights/DFW-SEA/0600/Economy", "30".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		    zk.create("/flights/SFO-JFK/0600/First", "30".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		    zk.create("/flights/SFO-JFK/0600/Business", "30".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		    zk.create("/flights/SFO-JFK/0600/Economy", "30".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		    zk.create("/flights/DEL-MAA/0600/First", "30".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		    zk.create("/flights/DEL-MAA/0600/Business", "30".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		    zk.create("/flights/DEL-MAA/0600/Economy", "30".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		    zk.create("/booking-order", "flights".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		    zk.create("/booking-ids", "flights".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		    zk.create("/status", "flights".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		    
		} catch(InterruptedException intrEx) {
		    System.out.println("");
		} catch(KeeperException kpEx) {
		    System.out.println("");
		}
		
	
	}
}
