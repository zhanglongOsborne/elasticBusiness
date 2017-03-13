package zookeeper.consumer;

import java.util.ArrayList;


import org.apache.zookeeper.AsyncCallback.VoidCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

import zookeeper.connect.ZkClient;
import zookeeper.utils.Utils;

public class Consumer {
	private ZkClient zk;
	private ArrayList<String> consumeBussiList;  //消费的business集合。
	private String myName;
	
	private ArrayList<String> createdConsumeBussiList;
	
	public Consumer(ZkClient zk, ArrayList<String> consumeBussiList,String myName) {
		super();
		this.zk = zk;
		this.consumeBussiList = consumeBussiList;
		this.myName = myName;
		createdConsumeBussiList = new ArrayList<>();
	}
	
	public boolean setConsumeBussiness(String path,String lockPath) throws KeeperException, InterruptedException{
		Utils utils = new Utils();
		utils.getLock(zk, lockPath,"set consumer");
		
		while(!utils.isFlag()) Thread.sleep(100);
		
		try {
			String Result = zk.getzKeeper().create(path, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
			//创建的是临时节点。当会话结束或者过期就会被删除
			utils.deleCreatedNode(zk);
			if(Result.equals(path))
				return true;
			else
				return false;
		} catch (KeeperException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;
	}
	
	public void deleteConsumeBussiness(String path) throws KeeperException, InterruptedException{
		Utils.recurseDeleteNode(zk, path);
		createdConsumeBussiList.remove(path);
	}
	
	public void deleteAllConsumeBussiness() throws KeeperException, InterruptedException{
		String path;
		for(String tmp:createdConsumeBussiList){
			path = "/zookeeper/businessSet/"+tmp+"/temporary/"+myName;
			Utils.recurseDeleteNode(zk, path);
			createdConsumeBussiList.remove(tmp);
		}
	}
	
	public void setAllConsumeBussiList() throws KeeperException, InterruptedException{
		String path;
		String fatherPath;
		String lockPath;
		for(String tmp:consumeBussiList){
			fatherPath = "/zookeeper/businessSet/"+tmp+"/temporary";
			lockPath = "/zookeeper/businessSet/"+tmp+"/lockNode";
			if(zk.getzKeeper().exists(fatherPath, false) == null){
				System.out.println("the father node is not existed :"+fatherPath);//这里是为了判断要消费的business是否存在。
				continue;
			}
			path = fatherPath + "/"+myName;
			System.out.println("add new node to temporary node: "+path);
			if(!setConsumeBussiness(path,lockPath))
				System.out.println("failed to set consumer to temporary node, the failed path is:"+path);
			createdConsumeBussiList.add(tmp);
		}
	}
	
}
