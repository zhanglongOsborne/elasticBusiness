package zookeeper.utils;

import org.apache.zookeeper.ZooKeeper;

import java.awt.List;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;

import org.apache.zookeeper.AsyncCallback.VoidCallback;

import zookeeper.connect.ZkClient;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;

public class Utils {
	private boolean flag = false;
	private String createdNode;
	
	
	public boolean isFlag() {
		return flag;
	}


	public String getCreatedNode() {
		return createdNode;
	}
	
	public void deleCreatedNode(ZkClient zk) throws InterruptedException, KeeperException{
		zk.getzKeeper().delete(createdNode, -1);
		System.out.println("delete the created lock node -----"+createdNode);
	}

	public static void recurseDeleteNode(final ZkClient zk,String path) throws KeeperException, InterruptedException{
		boolean result = false;
		final VoidCallback cb = new VoidCallback() {

			@Override
			public void processResult(int rc, String path, Object ctx) {
				// TODO Auto-generated method stub
				if(ctx.equals(path)){
					System.out.println("succeed to delete node: "+path);
				}
				else{
					System.out.println("failed to delete node:"+path);
					System.out.println("try again to delete node: "+path);
					//如果没有删除成功则最后尝试一次
					try {
						zk.getzKeeper().delete((String)ctx, -1);
					} catch (InterruptedException | KeeperException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}

		};
		
		java.util.List<String> childrenList = zk.getzKeeper().getChildren(path, false);
		if(childrenList.isEmpty()){
			zk.getzKeeper().delete(path, -1, cb, path);
		}
		/**
		 * 当存在孩子节点的时候
		 */
		for(String pathCd:childrenList){
			String newPath = "";
			if(path.equals("/")){
				newPath = "/"+pathCd;
			}else{
				newPath = path+"/"+pathCd;
			}
			recurseDeleteNode(zk, newPath);
		}
		zk.getzKeeper().delete(path, -1,cb,path);
	}
	
	/*
	 * 下面的函数用来获得锁，具体步骤如下：
	 * 1. 调用create方法来创建一个路径为locknode/lock- 的节点，此节点类型为sequence何ephemeral。
	 * 2. 在创建的锁节点上调用getChildren方法，来获取锁目录下的最小编号节点，并且不设置watcher。
	 * 3. 步骤2中获取的最小节点恰好是步骤1中客户端创建的节点，那么此客户端获得此种类型的锁，进行一系列操作后退出。
	 * 4. 客户端在所目录上调用exists方法，并且设置watcher来监视所目录下比自己小一个的连续节点的状态。
	 * 5. 如果步骤4中监视的节点发生变化，课跳转第2步，继续进行后续的操作，直到退出锁竞争。
	 */
	
	public void getLock(final ZkClient zk,final String lockPath,String contex) throws KeeperException, InterruptedException{
		createdNode = zk.getzKeeper().create(lockPath+"/lock-", null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
		System.out.println("the created lock node is ++++"+createdNode+"======>"+contex);
		
		checkNowLockNumber(zk, lockPath, createdNode);
	}
	
	private void checkNowLockNumber(final ZkClient zk,final String lockPath,String createdPath) throws KeeperException, InterruptedException{
		ArrayList<String> childrenNode = getSortedChildrenList(zk, lockPath);
		if(childrenNode.get(0).equals(createdNode.substring(createdNode.lastIndexOf("/")+1))){
			flag = true;
		}else{
			int index = childrenNode.indexOf(createdNode.substring(createdNode.lastIndexOf("/")+1));
			if(index !=-1 && index>0){
				Watcher watcher = new Watcher() {
					
					@Override
					public void process(WatchedEvent arg0) {
						// TODO Auto-generated method stub
						try {
							ArrayList<String> child = getSortedChildrenList(zk, lockPath);
							if(child.get(0).equals(createdNode.substring(createdNode.lastIndexOf("/")+1))){
								flag = true;
								return ;
							}else{
								int index = child.indexOf(createdNode.substring(createdNode.lastIndexOf("/")+1));
								System.out.println("the index is :"+index+" and the create node is"+createdNode);
								zk.getzKeeper().exists(lockPath+"/"+child.get(index-1),this);
							}
						} catch (KeeperException | InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				};
				if(zk.getzKeeper().exists(lockPath+"/"+childrenNode.get(index-1),watcher)==null){
					checkNowLockNumber( zk,lockPath,createdPath);
				}
			}
			
		}
	}
	
	public static ArrayList<String> getSortedChildrenList(ZkClient zk,String path) throws KeeperException, InterruptedException{
		ArrayList<String> childrenNode = (ArrayList<String>) zk.getzKeeper().getChildren(path, false);
		Collections.sort(childrenNode, new Comparator<String>() {

			@Override
			public int compare(String o1, String o2) {
				// TODO Auto-generated method stub
				int t1 = Integer.parseInt(o1.substring(o1.lastIndexOf("-")+1));
				int t2 = Integer.parseInt(o2.substring(o2.lastIndexOf("-")+1));
				return t1-t2;
			}
		});
		return childrenNode;
	}
};
