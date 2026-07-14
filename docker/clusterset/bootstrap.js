// InnoDB ClusterSet Bootstrap Script
// Creates a primary cluster (3 nodes) and a replica cluster (3 nodes) in a ClusterSet

var primaryNodes = ["172.28.2.2:3306", "172.28.2.3:3306", "172.28.2.4:3306"];
var replicaNodes = ["172.28.2.5:3306", "172.28.2.6:3306", "172.28.2.7:3306"];
var allNodes = primaryNodes.concat(replicaNodes);
var password = "root";

// Configure all instances for InnoDB Cluster
print("\n=== Configuring instances ===\n");
for (var i = 0; i < allNodes.length; i++) {
    var uri = "root:" + password + "@" + allNodes[i];
    print("Configuring " + allNodes[i] + "...\n");
    dba.configureInstance(uri, {clusterAdmin: "icadmin", clusterAdminPassword: "icadmin", restart: false});
}

// Create the primary InnoDB Cluster on node1
print("\n=== Creating primary cluster ===\n");
shell.connect("icadmin:icadmin@" + primaryNodes[0]);
var cluster = dba.createCluster("primary_cluster");

// Add remaining primary nodes
for (var i = 1; i < primaryNodes.length; i++) {
    print("Adding " + primaryNodes[i] + " to primary cluster...\n");
    cluster.addInstance("icadmin:icadmin@" + primaryNodes[i], {recoveryMethod: "clone"});
}

// Wait for all primary cluster members to be ONLINE
print("\n=== Waiting for primary cluster to be ready ===\n");
var ready = false;
while (!ready) {
    var status = cluster.status();
    var allOnline = true;
    var topology = status.defaultReplicaSet.topology;
    for (var addr in topology) {
        if (topology[addr].status !== "ONLINE") {
            allOnline = false;
            break;
        }
    }
    if (allOnline && Object.keys(topology).length === 3) {
        ready = true;
    } else {
        os.sleep(2);
    }
}
print("Primary cluster ready with 3 ONLINE members.\n");

// Create the ClusterSet
print("\n=== Creating ClusterSet ===\n");
var clusterset = cluster.createClusterSet("global_clusterset");

// Create the replica cluster on node4
print("\n=== Creating replica cluster ===\n");
var replicaCluster = clusterset.createReplicaCluster("icadmin:icadmin@" + replicaNodes[0], "dr_cluster", {recoveryMethod: "clone"});

// Add remaining replica nodes
for (var i = 1; i < replicaNodes.length; i++) {
    print("Adding " + replicaNodes[i] + " to replica cluster...\n");
    replicaCluster.addInstance("icadmin:icadmin@" + replicaNodes[i], {recoveryMethod: "clone"});
}

// Wait for replica cluster members
print("\n=== Waiting for replica cluster to be ready ===\n");
ready = false;
while (!ready) {
    var status = replicaCluster.status();
    var allOnline = true;
    var topology = status.defaultReplicaSet.topology;
    for (var addr in topology) {
        if (topology[addr].status !== "ONLINE") {
            allOnline = false;
            break;
        }
    }
    if (allOnline && Object.keys(topology).length === 3) {
        ready = true;
    } else {
        os.sleep(2);
    }
}
print("Replica cluster ready with 3 ONLINE members.\n");

// Print final ClusterSet status
print("\n=== ClusterSet Status ===\n");
shell.connect("icadmin:icadmin@" + primaryNodes[0]);
var cs = dba.getClusterSet();
print(cs.status({extended: 1}));

print("\n=== Bootstrap complete ===\n");

// Keep container alive
while (true) {
    os.sleep(3600);
}
