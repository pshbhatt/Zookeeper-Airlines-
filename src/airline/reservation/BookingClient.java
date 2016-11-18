package airline.reservation;

import java.io.IOException;
import java.text.NumberFormat;
import java.text.ParsePosition;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Shell.ExitCodeException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;

/**
 * @author Piyush, Himat
 * Date: 03/30/016
 * BookingClient.java
 * This Class performs the operations based on the arguments passed from the CLI.
 * The status of the flights can be checked (seat availability), tickets can be booked and cancelled.
 */
public class BookingClient {
	
	/**
	 * Class level variables:
	 * Zookeeper object
	 */
	static ZooKeeper zk = null;
/**
 * @param args
 * @throws NumberFormatException
 * @throws KeeperException
 * @throws InterruptedException
 * This method performs the required validations based on the arguments passed using command line interface,
 * If the argument passed for operation is status, it shows the status.
 * if the argument passed for operation is booking, it takes other data like Flight route, Time, class
 * and quantity to book the ticket. The seats are updated for the particular flight and class accordingly.
 * If the argument passed for operation is cancel, it uses the booking ID passed and cancel the seats.
 * It updates the seat availability accordingly.
 */
/**
 * @param args
 * @throws NumberFormatException
 * @throws KeeperException
 * @throws InterruptedException
 */
/**
 * @param args
 * @throws NumberFormatException
 * @throws KeeperException
 * @throws InterruptedException
 */
public static void main(String[] args) throws NumberFormatException, KeeperException, InterruptedException {
	Logger.getRootLogger().setLevel(Level.OFF);
	String quantity = null;
	String bookingId=null;
	String classInterested = null;
	
	String portNumber = null;
	String operation =  null;
	String route = null;
	String time = null;
	//Validation for the required arguments passed.
	try{
	portNumber = args[1];
	operation = args[3];
	} catch(Exception e){
		System.out.println("In order to invoke Booking Client please enter Port Number and operation in the following format:\n --portNum <portNumber> -- operation <status/booking/cancel>");
		System.exit(0);
	}
	//Validation for the validity of the port number.
	try {
		zk=Initializer.connect("127.0.0.1:"+portNumber);
	} catch (IOException e1) {
		e1.printStackTrace();
	} catch (InterruptedException e1) {
		e1.printStackTrace();
	} catch (NumberFormatException e1) {
		System.out.println("Please enter Port Number to access the booking client using the following format --portNum<number>");
	}
	//If the argument passed as operation is Status.
	 if(operation.equals("status")){
	//Validation for arguments passed when the operation is status.
		 try {
		
		route = args[5];
	} catch (Exception e){
		System.out.println("Flight route missing. In order to check the status please provide the flight route");
		System.exit(0);
	}
	//Validation for the flight route passed. It check whether the route passed as an arguments is present in the list.
	if(route.equals("ORD-OKC")||route.equals("EWR-TUL")||route.equals("DFW-SEA")||route.equals("SFO-JFK")||route.equals("DEL-MAA")){
	} else {
		System.out.println(route + " not found. Please use one of the following routes:\nORD-OKC\nEWR-TUL\nDFA-SEA\nSFO-JFK\nDEL-MAA");
		System.exit(0);
	}
	//If the argument passed as operation is Booking.
	 } else if(operation.equals("booking")||operation.equals("Booking")){
		//Validation for arguments passed when the operation is booking.
		 try{
			route = args[5];
		quantity = args[7];
		classInterested = args[9];
		time = args[11];
		} catch (Exception e){
			System.out.println("In order to book the flight please provide the Quantity, Route, Class Interested and flight schedule as:\n --flight.route<route> --quantity <quantity(number) --classInterested<First/Business/Economy> --time<1700/600>");
			System.exit(0);
		}
		//Validation if the quantity passed has a numeric value. 
		if(!isNumeric(quantity)){
			System.out.println(quantity + " is an invalid quantity. Please use a numeric value for quantity");
			System.exit(0);
		}
		//Validation for Class Interested passed as an argument. It checks whether the class is First or Business or Economy.
		if(classInterested.equals("First")||classInterested.equals("first")||classInterested.equals("Business")||classInterested.equals("business")||classInterested.equals("Economy")||classInterested.equals("economy")){
		} else {
			System.out.println(classInterested + " is not a valid airline seating class. Please use one of the following classes:\nFirst\nBusiness\nEconomy");
			System.exit(0);
		}
		//Validation for the flight route passed. It check whether the route passed as an arguments is present in the list.
		if(route.equals("ORD-OKC")||route.equals("EWR-TUL")||route.equals("DFW-SEA")||route.equals("SFO-JFK")||route.equals("DEL-MAA")){
		} else {
			System.out.println(route + " not found. Please use one of the following routes:\nORD-OKC\nEWR-TUL\nDFA-SEA\nSFO-JFK\nDEL-MAA");
			System.exit(0);
		}
	}
	 //If the argument passed as the operation is Cancel.
	 else if(operation.equals("Cancel")|| operation.equals("cancel")){
		//Validation for arguments passed when the operation is booking.	
		 try{
		bookingId = args[5];
		} catch (Exception e){
			System.out.println("To cancel the ticket please specify the booking Id in the following format:\n--bookingId<Id>");
		}
	//Validation if the operations passed is not Status or Booking or Cancel.	
	} else {
		System.out.println(operation  + " not found. Please enter one of the following:\n'Status' to know the status of the flight.\n'Booking' to book a new flight\n'Cancel' to cancel an existing booking");
		System.exit(0);
	}
	Initializer.createZnodes();
	
	//Checking and displaying the status of the flight. It shows the availability of seats for all the flights
	//and the time of the flights along with the classes. It also waits for the write operation before it can read (Extra Bonus)
	if(operation.equals("status")|| operation.equals("Status")){
		List<String> children = new ArrayList<String>();
		children=zk.getChildren("/booking-order", new Watcher() {       // Anonymous Watcher
		    @Override
		    public void process(WatchedEvent event) {
		       // check for event type NodeCreated
		       boolean isNodeCreated = event.getType().equals(EventType.NodeDeleted);
		    }
		});
		zk.create("/booking-order/s", route.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
		try {
			System.out.println("For route " + route + " at time 1700 hours"+ " Total seats: "+new String(zk.getData("/flights/"+route + "/1700", false, null)));
			System.out.println("For route " + route + " at time 1700 hours"+ " Seats in First Class "+new String(zk.getData("/flights/"+route+"/1700" + "/First", false, null)));
			System.out.println("For route " + route + " at time 1700 hours"+ " Seats in Busines Class: "+new String(zk.getData("/flights/"+route+"/1700" + "/Business", false, null)));
			System.out.println("For route " + route + " at time 1700 hours"+ " Seats in Economy Class: "+new String(zk.getData("/flights/"+route+"/1700" + "/Economy", false, null)));
			System.out.println("For route " + route + " at time 0600 hours"+ " Total seats: "+new String(zk.getData("/flights/"+route + "/0600", false, null)));
			System.out.println("For route " + route + " at time 0600 hours"+ " Seats in First Class "+new String(zk.getData("/flights/"+route+"/0600" + "/First", false, null)));
			System.out.println("For route " + route + " at time 0600 hours"+ " Seats in Busines Class: "+new String(zk.getData("/flights/"+route+"/0600" + "/Business", false, null)));
			System.out.println("For route " + route + " at time 0600 hours"+ " Seats in Economy Class: "+new String(zk.getData("/flights/"+route+"/0600" + "/Economy", false, null)));
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		//The ticket is booked in this section and the seats are updated according to the quantity passed as the argument.
	} else if (operation.equals("booking")||operation.equals("Booking")){
		List<String> children = new ArrayList<String>();
		children=zk.getChildren("/booking-order", new Watcher() {       // Anonymous Watcher
		    @Override
		    public void process(WatchedEvent event) {
		       // check for event type NodeCreated
		       boolean isNodeCreated = event.getType().equals(EventType.NodeDeleted);
		    }
		});
		
		zk.create("/booking-order/o", "newChild".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
		System.out.println("Booking Order: ");
		
		children=zk.getChildren("/booking-order", false);
		for(int i=0;i<children.size();i++){
			System.out.println(children.get(i));
		}
		
		if(Integer.parseInt(quantity)<=Integer.parseInt((new String(zk.getData("/flights/" + route+"/"+time+"/" + classInterested, false, null))))){
			zk.create("/booking-ids/id", (route+"#"+classInterested+"#"+quantity+"#"+time).getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
		} else {
			System.out.println("Seats requested are not available");
		}
		
		List<String> childrenId = new ArrayList<String>();
		System.out.println("Booking-Ids:");
		childrenId=zk.getChildren("/booking-ids", false);
		for(int i=0;i<childrenId.size();i++){
			if(bookingId == childrenId.get(i)){
				zk.delete(childrenId.get(i), -1);
			}
			
			System.out.println(childrenId.get(i));
		}
		String quantityLeftClass = new String(zk.getData("/flights/"+route+"/"+time+"/"+classInterested,false, null));
		String quantityLeft = new String(zk.getData("/flights/"+route+"/"+time,false, null));
		zk.setData("/flights/"+route+"/"+time+"/"+classInterested, (String.valueOf(Integer.parseInt(quantityLeftClass)-Integer.parseInt(quantity))).getBytes(), -1);
		zk.setData("/flights/"+route+"/"+time, (String.valueOf(Integer.parseInt(quantityLeft)-Integer.parseInt(quantity))).getBytes(), -1);
		//Tickets are cancelled in this section and seats are updated according to the quantity booked.
	} else if (operation.equals("cancel")||operation.equals("Cancel")){
		
		List<String> childrenId = new ArrayList<String>();
		childrenId=zk.getChildren("/booking-ids", false);
		int index = childrenId.indexOf(bookingId);
		if(index == -1){
			System.out.println("Invalid Booking Id");
		} else {
			zk.create("/booking-order/o", "newChild".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
			List<String> children = new ArrayList<String>();
			children=zk.getChildren("/booking-order", false);
			for(int i=0;i<children.size();i++){
				System.out.println(children.get(i));
			}
			String flightData = new String (zk.getData("/booking-ids/"+bookingId, false, null));
			String flightDataAr[] = flightData.split("#"); 
			String cRoute = flightDataAr[0];
			String cQuantity = flightDataAr[2];
			String cClass = flightDataAr[1];
			String cTime = flightDataAr[3];
			
			String quantityLeftClass = new String(zk.getData("/flights/"+cRoute+"/"+cTime + "/"+cClass,false, null));
			String quantityLeft = new String(zk.getData("/flights/"+cRoute + "/"+cTime,false, null));
			if(Integer.parseInt(quantityLeftClass)<30){
			zk.setData("/flights/"+cRoute+"/"+cTime+"/"+cClass, (String.valueOf(Integer.parseInt(quantityLeftClass)+Integer.parseInt(flightData.substring(flightData.length()-1, flightData.length())))).getBytes(), -1);
			zk.setData("/flights/"+cRoute+"/"+cTime, (String.valueOf(Integer.parseInt(quantityLeft)+Integer.parseInt(flightData.substring(flightData.length()-1, flightData.length())))).getBytes(), -1);
			zk.delete("/booking-ids/" + bookingId, -1);
			System.out.println("Ticket(s) have been successfully cancelled");
			}
		}
		
	}
}

/**
 * @param str
 * @return
 * This checks whether the String passed is numeric or not.
 */
public static boolean isNumeric(String str)
{
  NumberFormat formatter = NumberFormat.getInstance();
  ParsePosition pos = new ParsePosition(0);
  formatter.parse(str, pos);
  return str.length() == pos.getIndex();
}
}
