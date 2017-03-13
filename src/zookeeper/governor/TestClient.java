package zookeeper.governor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import javax.print.attribute.standard.RequestingUserName;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

import zookeeper.connect.ZkClient;
import zookeeper.consumer.Business;
import zookeeper.consumer.Consumer;
import zookeeper.producer.Producer;

/**
 * 
 * @author osborne
 * 这个类是用来进行系统测试的。主要测试点：
 * 1.创建若干个producer（增加，删除）
 * 2.创建若干个business（增加，删除）
 * 3.创建若干个消费者（每个消费者消费多个business）
 * 4.持续连接，断开连接，模拟 sessionId timeout 的情形。
 *
 */
public class TestClient {
	private ZkClient zk;
	private int producerNum;
	private int businessNum;
	private int consumerNum;
	private HashMap<String, ArrayList<String>> producerMap;
	private ArrayList<String> businessList;
	private HashMap<String, ArrayList<String>> consumerList;
	
	private HashMap<String, Producer> producers;
	private HashMap<String, Business> businesses;
	private HashMap<String, Consumer> consumers;
	
	
	public TestClient(ZkClient zk, int producerNum, int businessNum, int consumerNum, HashMap<String, ArrayList<String>> producerMap,
			ArrayList<String> businessList, HashMap<String, ArrayList<String>> consumerList) {
		super();
		this.zk = zk;
		this.producerNum = producerNum;
		this.businessNum = businessNum;
		this.consumerNum = consumerNum;
		this.producerMap = producerMap;
		this.businessList = businessList;
		this.consumerList = consumerList;
	}
	
	public void initAllNode() throws KeeperException, InterruptedException{
		initialCreateBusiness();//先生成business节点，应为producer节点需要设置business子节点stable的watcher
		initialCreateProducer();
		initialCreateConsumer();
	}

/**
 * 这个函数是用来初始化producer节点的，根据producerList创建所有的producer节点。
 * @throws InterruptedException 
 * @throws KeeperException 
 */
	public void initialCreateProducer() throws KeeperException, InterruptedException{
		String path;
		Iterator iterator = producerMap.entrySet().iterator();
		producers = new HashMap<>();
		while(iterator.hasNext()){
			Map.Entry<String, ArrayList<String>> entry = (Entry<String, ArrayList<String>>) iterator.next();
			String producer = entry.getKey();
			ArrayList<String> busiList = entry.getValue();
			path = "/zookeeper/producerSet/"+producer;
			Producer P = new Producer(zk, path, busiList);
			/**
			 * 创建producer节点并且对每个Producer节点挂上business节点，并且保存每个Producer对象，便于以后的增删操作。
			 */
			P.createNewProducer();
			P.run();
			producers.put(producer, P);
		}
	}
	
/**
 * 这个函数是用来初始化business节点的
 * @throws InterruptedException 
 * @throws KeeperException 
 */
	public void initialCreateBusiness() throws KeeperException, InterruptedException{
		String path;
		businesses = new HashMap<>();
		for(String tmp:businessList){
			path = "/zookeeper/businessSet/"+tmp;
			Business b = new Business(zk, path, path+"/stable", path+"/temporary");
			b.creatNewBusiness();
			businesses.put(tmp, b);
		}
	}
	
/**
 * 这个函数是用来初始化consumer的，将consumer挂到temporary节点上。
 * @throws InterruptedException 
 * @throws KeeperException 
 */
	public void initialCreateConsumer() throws KeeperException, InterruptedException{
		Iterator iterator = consumerList.entrySet().iterator();
		consumers = new HashMap<>();
		while(iterator.hasNext()){
			Map.Entry<String, ArrayList<String>> entry = (Entry<String, ArrayList<String>>) iterator.next();
			String consumerName = entry.getKey();
			ArrayList<String> consumeBusiList = entry.getValue();
			Consumer c = new Consumer(zk, consumeBusiList, consumerName);
			c.setAllConsumeBussiList();
			consumers.put(consumerName, c);
		}
	}
	
	/**
	 * @throws InterruptedException 
	 * @throws KeeperException 
	 * 这里是删除节点
	 */
	public void deleteProducer(String producer) throws KeeperException, InterruptedException{
		producers.get(producer).deleProducer();
		producers.remove(producer);
	}
	
	public void deleteBusiness(String business) throws KeeperException, InterruptedException{
		businesses.get(business).deleteBusiness();
		businesses.remove(business);
	}
	
	public void deleteConsumer(String consumer) throws KeeperException, InterruptedException{
		consumers.get(consumer).deleteAllConsumeBussiness();
		consumers.remove(consumer);
	}
	
	/**
	 * 这里是增加节点
	 * @throws InterruptedException 
	 * @throws KeeperException 
	 */
	public boolean addProducer(String producer,ArrayList<String>ownedBusinessList) throws KeeperException, InterruptedException{
		if(producers.containsKey(producer)){
			System.out.println("the producer you want to add is already existed  :"+producer);
			return false;
		}
		String producerPath = "/zookeeper/ProducerSet/"+producer;
		Producer pro = new Producer(zk,producerPath, ownedBusinessList);
		pro.createNewProducer();
		pro.addInitialBusiness();
		producers.put(producer, pro);
		return true;
	}
	
	public boolean addBusiness(String business) throws KeeperException, InterruptedException{
		if(businesses.containsKey(business)){
			System.out.println("the business you want to add is already existed  :"+business);
			return false;
		}
		String path = "/zookeeper/businessSet/"+business;
		Business b = new Business(zk, path, path+"/stable", path+"/temporary");
		b.creatNewBusiness();
		businesses.put(business, b);
		return true;
	}
	
	public boolean addConsumer(String consumer,ArrayList<String> consumeBusinessList) throws KeeperException, InterruptedException{
		if(consumers.containsKey(consumer)){
			System.out.println("the consumer you want to add is already existed  :"+consumer);
			return false;
		}
		Consumer c = new Consumer(zk, consumeBusinessList, consumer);
		c.setAllConsumeBussiList();
		consumers.put(consumer, c);
		return true;
	}
	
	
	
};
