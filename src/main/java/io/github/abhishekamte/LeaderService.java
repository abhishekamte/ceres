package io.github.abhishekamte;

import org.apache.log4j.Logger;
import org.apache.zookeeper.*;

import java.net.InetAddress;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class LeaderService {

    // Constants for use
    public final static String ZK_CONN_STRING = "localhost:2181";
    public final static String ZK_APP_PREFIX = "/ceres";
    public final static String ZK_PREFIX_PATH = ZK_APP_PREFIX + "/leader";
    private final static Logger logger = Logger.getLogger(LeaderService.class.getName());
    private ZooKeeper zkClient;

    private String currentLeader = null;
    private String myNodeId = null;

    public LeaderService() {
        try {
            logger.info("Connecting to zk with connection string localhost:2181");
            final CountDownLatch connectionLatch = new CountDownLatch(1);
            zkClient = new ZooKeeper(ZK_CONN_STRING, 2000, we -> {
                if (we.getState() == Watcher.Event.KeeperState.SyncConnected) {
                    connectionLatch.countDown();
                }
            });

            while (connectionLatch.getCount() != 0) {
                logger.debug("Waiting for zk connection to complete");
                connectionLatch.await(100, TimeUnit.MILLISECONDS);
            }

            if (zkClient.exists(ZK_PREFIX_PATH, false) == null) {
                logger.info("Parent node " + ZK_PREFIX_PATH + " is not set up. Creating it...");
                if (zkClient.exists(ZK_APP_PREFIX, false) == null) {
                    zkClient.create(ZK_APP_PREFIX, "ceres".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                }
                zkClient.create(ZK_PREFIX_PATH, "leader".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
            myNodeId = zkClient.create(ZK_PREFIX_PATH + "/node", InetAddress.getLocalHost().getHostName().getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
            electAndWatch();
        } catch (Exception ex) {
            logger.error("Failed to create node,", ex);
            ex.printStackTrace();
        }
        onStateChanged();
        logger.debug("LeaderService init complete");
    }

    private void electAndWatch() {

        try {
            zkClient.addWatch(ZK_PREFIX_PATH, event -> {
                logger.info("Leader watcher called");
                if (event.getType() == Watcher.Event.EventType.NodeCreated) {
                    logger.info("New node " + event.getPath() + " has joined");
                    onStateChanged();
                } else if (event.getType() == Watcher.Event.EventType.NodeDeleted) {
                    if(!event.getPath().equals(myNodeId)){
                        logger.info("Node at " + event.getPath() + " has left");
                        onStateChanged();
                    }else{
                        logger.info("I am leaving the cluster. Bye!");
                    }
                }
            }, AddWatchMode.PERSISTENT_RECURSIVE);
        } catch (KeeperException.SessionExpiredException e) {
            logger.info("Session has closed");
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    private void onStateChanged() {
        try {
            logger.info("Fetching all the nodes");
            final List children = zkClient.getChildren(ZK_PREFIX_PATH, false);
            logger.info("List of nodes: " + children);
            String newLeader = ZK_PREFIX_PATH + "/" + electLeader(children);
            if (currentLeader == null || !currentLeader.equals(newLeader)) {
                updateLeader(newLeader);
                logger.info("Leader has changed. Newly elected leader is " + newLeader);
                if (amILeader()) {
                    logger.info("Yay! I am the new leader. Peforming leader actions");
                    leaderActions();
                }
            } else {
                logger.info("Leader has not changed");
            }
        } catch (KeeperException.SessionExpiredException e) {
            logger.info("Session has closed");
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private String electLeader(List<String> nodes) {
        Collections.sort(nodes);
        return nodes.get(0);
    }

    private void updateLeader(String newLeader) {
        if (newLeader != null && !newLeader.isBlank()) {
            currentLeader = newLeader;
            logger.info("current leader: " + currentLeader);
        }
    }

    private boolean amILeader() {
        return currentLeader.equals(myNodeId);
    }

    public void leaderActions() {
        logger.info("Making empty promises......");
        logger.info("Bureaucracy......");
        logger.info("bureaucracy......");
        logger.info("bureaucracy......");
    }

    public void close() {
        logger.info("Closing connection to zk");
        try {
            zkClient.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
