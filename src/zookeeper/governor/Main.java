package zookeeper.governor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;

import zookeeper.connect.ZkClient;


public class Main {
	private static int producerNum = 100;
	private static int businessNum = 100;
	private static int consumerNum = 100;
	
	private static HashMap<String, ArrayList<String>> producerMap = new HashMap<>();
	private static ArrayList<String> businessList = new ArrayList<>();
	private static HashMap<String, ArrayList<String>> consumerList = new HashMap<>();
	
	/**
	 * 
	 * @param args
	 * args 的格式为 producerNum, businessNum, consumerNum, producerMap, businessList, consumerList
	 * 其中producerMap：“pro1，bus1，bus2；pro2，bus3，bus4；”
	 * businessList:"bus1,bus2,bus3"
	 * consumerList:"consumer1,bus1,bus3;consumer2,bus2,bus4"
	 */
	public static void analyzeArgs(String [] args){
		producerNum = Integer.parseInt(args[0]);
		businessNum = Integer.parseInt(args[1]);
		consumerNum = Integer.parseInt(args[2]);
		
		//用来处理producerMap信息
		String[] producers = args[3].split(";");
		for(String tmp:producers){
			String [] oneSample = tmp.split(",");
			String key = oneSample[0];
			ArrayList<String> value = new ArrayList<>();
			for(int i=1;i<oneSample.length;i++){
				value.add(oneSample[i]);
			}
			producerMap.put(key, value);
		}
		
		//用来提取businessList信息
		for(String tmp:args[4].split(",")){
			businessList.add(tmp);
		}
		
		//用来提取consumerList信息
		String [] consumers = args[5].split(";");
		for(String tmp:consumers){
			String [] oneSample = tmp.split(",");
			String key = oneSample[0];
			ArrayList<String> value = new ArrayList<>();
			for(int i=1;i<oneSample.length;i++){
				value.add(oneSample[i]);
			}
			consumerList.put(key, value);
		}
	}
	
	public static void autoSetArgs(){
		ArrayList<String> p = constructSequenceString("business", 100);
		ArrayList<String> c = constructSequenceString("business", 100);
		for(int i=0;i<producerNum;i++){
			producerMap.put("producer"+Integer.toString(i), p);
		}
		businessList.addAll(constructSequenceString("business", businessNum));
		for(int i=0;i<consumerNum;i++){
			consumerList.put("consumer"+Integer.toString(i),c);
		}
	}
	
	public static ArrayList<String> constructSequenceString(String head,int num){
		ArrayList<String> result = new ArrayList<>();
		for(int i=0;i<num;i++){
			result.add(head+Integer.toString(i));
		}
		return result;
	}
	
	public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
		String host = "localhost:2181";
		int sessionTimeout = 2000;
		Watcher connectWatcher = new Watcher() {
			
			@Override
			public void process(WatchedEvent arg0) {
				// TODO Auto-generated method stub
				System.out.println("the connectwatcher gives information!");
			}
		};
		//analyzeArgs(args);
		autoSetArgs();
		ZkClient zk = new ZkClient(host, sessionTimeout);
		zk.connect();
		
		String productSetPath = "/zookeeper/producerSet";
		String businessSetPath = "/zookeeper/businessSet";
		zk.getzKeeper().create(productSetPath, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		zk.getzKeeper().create(businessSetPath, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		
		//Utils.recurseDeleteNode(zk, "/zookeeper/producerSet");
		//Utils.recurseDeleteNode(zk, "/zookeeper/businessSet");
		
		
		TestClient testClient = new TestClient(zk, producerNum, businessNum, consumerNum, producerMap, businessList, consumerList);
		testClient.initAllNode();
		
		while(true){
			//System.out.println("loop in main");
			Thread.sleep(10000);
		}
	}
}
