package zookeeper.consumer;


import com.google.common.collect.*;

import zookeeper.connect.ZkClient;
import zookeeper.utils.Utils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

public class TemporaryNodeWatcher implements Watcher{
	
	private ZkClient zk;
	private Collection<String> tempConsumerList;
	private KeepCalmThread keepCalmThread;
	private String stableNodePath;
	private String tmpNodePath;
	
	private Thread keepThread;
	public TemporaryNodeWatcher(ZkClient zk,String stableNodePath,String tmpNodePath,int looptime) {
		super();
		this.zk = zk;
		this.stableNodePath = stableNodePath;
		this.tmpNodePath = tmpNodePath;
		this.keepCalmThread = new KeepCalmThread(zk, looptime, tempConsumerList,tmpNodePath, stableNodePath);
		try {
			initTempConsumerList(zk, tmpNodePath);
		} catch (KeeperException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		keepThread = new Thread(keepCalmThread);
		keepThread.start();
	}
/**
	public TemporaryNodeWatcher(ZooKeeper zk,String stableNodePath,String tmpNodePath,KeepCalmThread keepCalm) {
		super();
		this.zk = zk;
		this.keepCalmThread = keepCalm;
		this.stableNodePath = stableNodePath;
		this.tmpNodePath = tmpNodePath;
		try {
			initTempConsumerList(zk, tmpNodePath);
		} catch (KeeperException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		this.keepCalmThread.run();
	}
**/
	
	public void destoryThread(){
		keepThread.interrupt();
	}
	
	public TemporaryNodeWatcher() {
		// TODO Auto-generated constructor stub
	}

	public void initTempConsumerList(ZkClient zk,String znode) throws KeeperException, InterruptedException{
		tempConsumerList = new ArrayList<String>();
		tempConsumerList = zk.getzKeeper().getChildren(znode, false);
		System.out.println("the initTempConsumerList size is:"+tmpNodePath+" "+tempConsumerList.size());
	}


	@Override
	public void process(WatchedEvent event) {
		// TODO Auto-generated method stub
		//当观察的路径是temporary节点下的子节点的时候，如果是节点被删除的话，则需要设置一个时间间隔，如果一段时间后仍然没有重新连接上的话，
		//则确认节点删除，然后删除stable中相对应的节点。若是在规定时间间隔中重新连接的话，则认为是因为网络问题导致的抖动，不做任何操作。
		//System.out.println("temporary node changed:"+tmpNodePath);
		switch(event.getType()){
    	case None:
    		processConState(event);
    		break;
    	case NodeChildrenChanged:
    		try {
    			processChildrenNodeChanged(event);
			} catch (KeeperException | InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    		break;
    	case NodeCreated:
    		System.out.println("node created++++++++++");
    		break;
    	case NodeDataChanged:
    		System.out.println("node changed+++++++++++");
    		break;
    	case NodeDeleted:
    		System.out.println("node deleted!+++++++++++");
    		processNodeDeleted(event);
    		break;
    	}
		/**
		 * 重新设置watcher
		 *
		try {
			zk.getzKeeper().getChildren(tmpNodePath, this);
			//System.out.println("set new temporaryWatcher! the watcher path is :"+tmpNodePath);
		} catch (KeeperException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		*/
	}

	private void processNodeDeleted(WatchedEvent event) {
		// TODO Auto-generated method stub
		System.out.println("the node is deleted!");
	}

/**
	private void processNodeChildrenChanged(WatchedEvent event) throws KeeperException, InterruptedException {
		// TODO Auto-generated method stub
		String changedPath = event.getPath();//这里获得的changedPath是父节点
		if(tempConsumerList.contains(changedPath)){ //该节点存在于本地保存节点中，说明之前已经加入到stable子节点中
			//这时候需要判断temporary节点的自己点中是否存在该节点，如果不存在该节点，则需要设置一个计时器，
			//如果该计时器结束时节点仍然不存在则认为该节点是被删除了，从而防止了抖动。
			if(zk.exists(changedPath, false) == null){//这说明节点是被删除的
				keepCalmThread.addNodeToMap(changedPath, 1);//将该节点加入到goneNodeMap中。
			}else{
				keepCalmThread.deleNodeFromMap(changedPath);
			}	
		}else{    //该节点不存在于本地本地节点中，说明该节点之前没有加入到stable子节点中。
			if(zk.exists(changedPath, false) != null){//说明新增加了一个节点。这时候需要向stable节点创建一个子节点。
				String newNode = tmpNodePath.substring(0,tmpNodePath.lastIndexOf("/")-1)+changedPath.substring(changedPath.lastIndexOf("/"));
				String createNode = zk.create(newNode, null,ZooDefs.Ids.READ_ACL_UNSAFE, CreateMode.PERSISTENT);
				if(createNode == newNode){
					System.out.println("create the new child node in stable node");
					synchronized (tempConsumerList) {
						tempConsumerList.add(newNode);
					}
				}else{
					System.out.println("failed in creating the new child node in stable node");
					//这里可能需要进一步的处理。
				}
			}
		}
	}
**/
	private void processChildrenNodeChanged(WatchedEvent event) throws KeeperException, InterruptedException{
		String lockPath = tmpNodePath.substring(0,tmpNodePath.lastIndexOf("/"))+"/lockNode";
		Utils utils = new Utils();
		utils.getLock(zk, lockPath,"the watcher process");
		
		while(!utils.isFlag()){
			//System.out.println("wating-------");
			Thread.sleep(100);
		}
		
		
		
		String changedPath = event.getPath();
		List<String> nowChildren = zk.getzKeeper().getChildren(changedPath, this);
		List<String> d1 = getTwoListDiff((List<String>) tempConsumerList, nowChildren);//返回存在于tempConsumerList 但是不存在于nowChildren
		List<String> d2 = getTwoListDiff(nowChildren, (List<String>) tempConsumerList);//返回存在于nowChildren，但是不存在于tempConsumerList
		
		if(!d1.isEmpty()){
			for(String tmp:d1){
				keepCalmThread.addNodeToGoneMap(tmp, 1);//这里不在tempConsumerList删除该节点，goneNode会删除。
			}
		}
		//System.out.println("d2.size()" + stableNodePath+" "+d2.size());
		if(!d2.isEmpty()){
			for(String tmp:d2){
				synchronized (tempConsumerList) {
					tempConsumerList.add(tmp);
				}
				System.out.println("add new node to creatingmap: "+stableNodePath+"/"+tmp);
				keepCalmThread.addNodeToCreatingMap(tmp, 1);
			}
		}
		System.out.println("<<<<<<<<<<<<<<");
		utils.deleCreatedNode(zk);
		
	}
	
	private List<String> getTwoListDiff(List<String> big,List<String> small){
		Set<String> diffSet = Sets.difference(Sets.newHashSet(big), Sets.newHashSet(small));
		return Lists.newArrayList(diffSet);
	}

	private void processConState(WatchedEvent event) {
		// TODO Auto-generated method stub
		switch(event.getState()){
		case AuthFailed://auth failed state
			System.out.println("你没有权限，请更改权限后再试");
			break;
		case Disconnected://the client is in the disconnected state.
			//重新进行连接，连接的过程需要设置一个timer来进行限制。
			break;
		case Expired://the server has expired this session.
			//这里需要进行重新连接处理，在出来的过程中连接一定次数或者连接一定时间后如果还是不能连接，说明网络存在问题。
			break;
		case SyncConnected:
			break;
		}
	}

}
