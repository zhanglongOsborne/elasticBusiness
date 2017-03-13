package zookeeper.consumer;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import zookeeper.connect.ZkClient;
import zookeeper.utils.Utils;

import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.AsyncCallback.VoidCallback;

/**
 * 
 * @author osborne
 * 每当创建一个业务的时候需要创建stable节点和temporary节点
 * 当在该业务下添加业务的时候需要添加在temporary节点下，并且创建的是临时节点而不是永久节点。
 *
 */
public class Business {
	private ZkClient zk;
	private String businessNodePath;
	private String stableNodePath;
	private String tmpNodePath;
	
	private String lockNodePath;
	
	private static int tollerantTime = 100;//这是抖动容忍时间间隔
	TemporaryNodeWatcher temporaryNodeWatcher;
	
	public Business(ZkClient zk, String newBusinessNode, String stableNodePath, String tmpNodePath) {
		super();
		this.zk = zk;
		this.businessNodePath = newBusinessNode;
		this.stableNodePath = stableNodePath;
		this.tmpNodePath = tmpNodePath;
	}
	
	public void creatNewBusiness() throws KeeperException, InterruptedException{
		zk.getzKeeper().create(businessNodePath, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		zk.getzKeeper().create(businessNodePath+"/stable",null,ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		zk.getzKeeper().create(businessNodePath+"/temporary",null,ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		zk.getzKeeper().create(businessNodePath+"/lockNode", null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		temporaryNodeWatcher = new TemporaryNodeWatcher(zk, stableNodePath, tmpNodePath,tollerantTime );
		try {
			System.out.println("set the temporaryNodeWatcher------the watcher path is :"+tmpNodePath);
			zk.getzKeeper().getChildren(tmpNodePath, temporaryNodeWatcher);
		} catch (KeeperException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	/**
	 * 当创建程序重新连接的时候，需要恢复一些操作；需要判断之前的创建的business节点是否还存在，如果不存在，则需要重新创建。
	 * @throws InterruptedException 
	 * @throws KeeperException 
	 */
	public void reConnect() throws KeeperException, InterruptedException{
		if(zk.getzKeeper().exists(businessNodePath, false) != null){
			temporaryNodeWatcher = new TemporaryNodeWatcher(zk, stableNodePath, tmpNodePath,tollerantTime );
			try {
				System.out.println("set the temporaryNodeWatcher------the watcher path is :"+tmpNodePath);
				zk.getzKeeper().getChildren(tmpNodePath, temporaryNodeWatcher);
			} catch (KeeperException | InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}else{
			creatNewBusiness();
		}
	}
	
	public void deleteBusiness() throws KeeperException, InterruptedException{
		//删除business之前需要先将维护线程停止
		temporaryNodeWatcher.destoryThread();
		Utils.recurseDeleteNode(zk, businessNodePath);
	}
	
	public void addNewTmpNode(String nodePath) throws KeeperException, InterruptedException{
		zk.getzKeeper().create(nodePath, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
	}
	
	public void deleteTmpNode(String nodePath) throws InterruptedException, KeeperException{
		zk.getzKeeper().delete(nodePath, -1);
	}


}
