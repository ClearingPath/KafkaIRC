/**
 * TODO : - Implement main menu and UI - Implement Thread for consumer (just run
 * consumer.consume()) - Implement user checking - Implement message constructor
 * & destructor (JSON Preferable)
 */
package com.lang.pat.kafkairc;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Scanner;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.json.simple.JSONObject;

/**
 *
 * @author ClearingPath
 */
public class ClientMain {

    public static String HOSTNAME = "localhost";
    public static int PORT = 2181;
    public static ArrayList<String> ChannelList = new ArrayList<>();
    public static String USERNAME;
    public static String QUEUENAME;
    public static String EXCHANGE_NAME = "lang.pat.rabbitIRC";
    public static boolean exit = false;
    public static String token;
    public static ThreadPoolExecutor executorPool;

    
    private Consumer consumer;
    private Producer producer;

    public ClientMain() {
	  System.out.println("* Init consumer threadpool...");
          RejectedExecutionHandlerImpl rejectionHandler = new RejectedExecutionHandlerImpl();        
	  ThreadFactory threadFactory = Executors.defaultThreadFactory();
	  executorPool = new ThreadPoolExecutor(5, 10, 100, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(1), threadFactory, rejectionHandler);
	  executorPool.allowCoreThreadTimeOut(true);
	  
	  System.out.println("* Consumer pool initialized successfully...");
	  System.out.println("* Init producer...");
	  producer = new Producer();
	  System.out.println("* Producer initialized successfully...");
	  CreateToken();
    }

    private void CreateToken() {
	  try {
		int randEnd = (int) (Math.random() * 99);
		Thread.sleep(randEnd);
		randEnd = (int) (Math.random() * 9999);
		String timestamp = String.valueOf(System.currentTimeMillis()) + randEnd;

		MessageDigest md = MessageDigest.getInstance("MD5");
		md.update(timestamp.getBytes());

		byte byteData[] = md.digest();

		StringBuffer sb = new StringBuffer();
		for (int i = 0; i < byteData.length; i++) {
		    sb.append(Integer.toString((byteData[i] & 0xff) + 0x100, 16).substring(1));
		}

		token = sb.toString();
	  } catch (NoSuchAlgorithmException ex) {
		Logger.getLogger(ClientMain.class.getName()).log(Level.SEVERE, null, ex);
	  } catch (InterruptedException ex) {
		Logger.getLogger(ClientMain.class.getName()).log(Level.SEVERE, null, ex);
	  }
    }

    public int JoinChannel(String Channel) {
	  for (String item : ChannelList) {
		if (item.equals(Channel)) {
		    return 1;
		}
	  }

	  ClientMain.ChannelList.add(Channel);
	  executorPool.execute(new Consumer(Channel));
	  producer.send(USERNAME + " has joined channel " + Channel + " !", Channel);
	  return 0;
    }

    public int LeaveChannel(String Channel) {
	  for (String item : ChannelList) {
		if (item.equals(Channel)) {
		    ClientMain.ChannelList.remove(Channel);
                    producer.send(USERNAME + " has leaved channel " + Channel + " !", Channel);
                    
		    return 0;
		}
	  }
	  return 1;
    }

    public int ChangeNick(String Nick) {
	  if (USERNAME.equals(Nick)) {
		return 1;
	  } else {
		ChannelList.clear();
		USERNAME = Nick;
		return 0;
	  }
    }

    public int Send(String Message) {
	  int ret = 0;
	  JSONObject JSONMessage = new JSONObject();
	  JSONMessage.put("username", USERNAME);
	  JSONMessage.put("message", Message);
	  JSONMessage.put("timestamp", System.currentTimeMillis());
	  JSONMessage.put("token", token);

	  producer.send(JSONMessage.toJSONString());
	  return ret;
    }

    public int Send(String Message, String ChannelName) {
	  int ret = 0;
	  JSONObject JSONMessage = new JSONObject();
	  JSONMessage.put("username", USERNAME);
	  JSONMessage.put("message", Message);
	  JSONMessage.put("timestamp", System.currentTimeMillis());
	  JSONMessage.put("token", token);

	  if (ChannelList.contains(ChannelName)) {
		producer.send(JSONMessage.toJSONString(), ChannelName);
	  } else {
		ret = 1;
	  }

	  return ret;
    }

    public static void Exit() {
//	  consumer.shutdown();
	  System.exit(0);
    }

    /**
     * @param args the command line arguments
     */
    public static void generateUname() {
	  String commonUsername[] = {"Earthshaker", "Sven", "Tiny", "Kunkka", "Beastmaster", "DragonKnight", "Axe", "Pudge", "SandKing", "Slardar", "Tidehunter", "WraithKing", "Bloodseeker", "Windranger", "StormSpirit", "Lina", "ShadowFiend", "AntiMage", "PhantomAssassin"};
	  String uname;
	  System.out.println("# Generating random username...");

	  int randIndex = (int) Math.round(Math.random() * (commonUsername.length - 1));
	  int randEnd = (int) (Math.random() * 999);
	  uname = commonUsername[randIndex] + randEnd;
	  System.out.println("# Generated new username: " + uname);

	  USERNAME = uname;
    }

    public static void main(String[] args) {
	  final ClientMain clientmain = new ClientMain();
	  if (args.length != 0) {
		System.out.println("# Set location of server according to first parameter as " + args[0]);
		HOSTNAME = args[0];
	  }
	  Scanner input = new Scanner(System.in);

	  generateUname();

	  while (!exit) {
		System.out.print("> ");

		String inputCommand = input.nextLine();
		String[] resSplit = inputCommand.split(" ", 2);
		String Command = resSplit[0].toUpperCase();
		int res;

		switch (Command) {
		    case "/NICK":
			  String newNick;
			  if (resSplit.length < 2) {
				System.out.print("# Enter new nickname: ");
				newNick = input.nextLine();
			  } else {
				newNick = resSplit[1];
			  }
			  res = clientmain.ChangeNick(newNick);
			  if (res == 1) {
				System.out.println("! Entered username is currently active username!");
			  } else if (res == 0) {
				System.out.println("# Username changed to " + resSplit[1]);
			  }
			  break;

		    case "/JOIN":
			  String chnName;
			  if (resSplit.length < 2) {
				System.out.print("# Enter channel name: ");
				chnName = input.nextLine();
			  } else {
				chnName = resSplit[1];
			  }
                          if (ChannelList.size() < 10){
                                res = clientmain.JoinChannel(chnName);
                                if (res == 0) {
                                      System.out.println("# User " + USERNAME + " has entered channel " + chnName);
                                } else if (res == 1) {
                                      System.out.println("! User " + USERNAME + " already entered channel " + chnName + "!");
                                } else {
                                      System.out.println("! User " + USERNAME + " failed to enter channel " + chnName + "!");
                                }
                          } else {
                              System.out.println("! User " + USERNAME + " has too many channel ! Please exit one channel to join another one !");
                          }
			  
			  break;

		    case "/LEAVE":
			  String chnName2;
			  if (resSplit.length < 2) {
				System.out.print("# Enter channel name: ");
				chnName2 = input.nextLine();
			  } else {
				chnName2 = resSplit[1];
			  }

			  res = clientmain.LeaveChannel(chnName2);
			  if (res == 0) {
				System.out.println("# User " + USERNAME + " has left channel " + chnName2);
			  } else {
				System.out.println("! User " + USERNAME + " failed to leave channel " + chnName2);
			  }
			  break;

		    case "/EXIT":
			  exit = true;
			  break;

		    default:
			  if (resSplit[0].startsWith("@")) {
				String msg, channelName;
				channelName = resSplit[0].substring(1);

				if (resSplit.length < 2) {
				    System.out.println("! No input message!");
				    System.out.print("# Masukkan message: ");
				    msg = input.nextLine();
				} else {
				    msg = resSplit[1];
				}

				res = clientmain.Send(msg, channelName);
				if (res == 1) {
				    System.out.println("! User " + USERNAME + " did not join channel " + channelName + "!");
				}
			  } else {
				res = clientmain.Send(inputCommand);
			  }
			  break;
		}
	  }

	  if (exit) {
		Exit();
	  }
    }

}
