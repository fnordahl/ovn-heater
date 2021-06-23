import ovn_context
import ovn_stats
import ovn_utils
import ovn_load_balancer as lb
import time
import netaddr
import random
import string
import copy
from collections import namedtuple
from randmac import RandMac
from datetime import datetime


ClusterConfig = namedtuple('ClusterConfig',
                           ['cluster_cmd_path',
                            'monitor_all',
                            'logical_dp_groups',
                            'clustered_db',
                            'node_net',
                            'node_remote',
                            'node_timeout_s',
                            'internal_net',
                            'external_net',
                            'gw_net',
                            'cluster_net',
                            'n_workers',
                            'vips',
                            'static_vips',])


BrExConfig = namedtuple('BrExConfig', ['physical_net'])


class Node(ovn_utils.Sandbox):
    def __init__(self, phys_node, container, mgmt_net, mgmt_ip):
        super(Node, self).__init__(phys_node, container)
        self.container = container
        self.mgmt_net = mgmt_net
        self.mgmt_ip = mgmt_ip

    def build_cmd(self, cluster_cfg, cmd, *args):
        monitor_cmd = 'OVN_MONITOR_ALL={}'.format(
            'yes' if cluster_cfg.monitor_all else 'no'
        )
        cluster_db_cmd = 'OVN_DB_CLUSTER={}'.format(
            'yes' if cluster_cfg.clustered_db else 'no'
        )
        cmd = "cd {} && " \
              "CHASSIS_COUNT=0 GW_COUNT=0 IP_HOST={} IP_CIDR={} IP_START={} " \
              "{} {} CREATE_FAKE_VMS=no ./ovn_cluster.sh {}".format(
                  cluster_cfg.cluster_cmd_path, self.mgmt_net.ip,
                  self.mgmt_net.prefixlen, self.mgmt_ip, monitor_cmd,
                  cluster_db_cmd, cmd
              )
        for i in args:
            cmd += ' {}'.format(i)
        return cmd


class CentralNode(Node):
    def __init__(self, phys_node, container, mgmt_net, mgmt_ip):
        super(CentralNode, self).__init__(phys_node, container,
                                          mgmt_net, mgmt_ip)

    def start(self, cluster_cfg):
        print('***** starting central node *****')
        self.phys_node.run(self.build_cmd(cluster_cfg, 'start'))
        time.sleep(5)


class WorkerNode(Node):
    def __init__(self, phys_node, container, mgmt_net, mgmt_ip,
                 int_net, ext_net, gw_net, unique_id):
        super(WorkerNode, self).__init__(phys_node, container,
                                         mgmt_net, mgmt_ip)
        self.int_net = int_net
        self.ext_net = ext_net
        self.gw_net = gw_net
        self.id = unique_id
        self.switch = None
        self.join_switch = None
        self.gw_router = None
        self.ext_switch = None
        self.lports = []

    def start(self, cluster_cfg):
        print('***** starting worker {} *****'.format(self.container))
        self.phys_node.run(self.build_cmd(cluster_cfg, 'add-chassis',
                                          self.container, 'tcp:0.0.0.1:6642'))

    @ovn_stats.timeit
    def connect(self, cluster_cfg):
        print('***** connecting worker {} *****'.format(self.container))
        self.phys_node.run(self.build_cmd(cluster_cfg,
                                          'set-chassis-ovn-remote',
                                          self.container,
                                          cluster_cfg.node_remote))

    def configure_localnet(self, physical_net):
        print('***** creating localnet on {} *****'.format(self.container))
        cmd = \
            'ovs-vsctl -- set open_vswitch . external-ids:{}={}:br-ex'.format(
                'ovn-bridge-mappings',
                physical_net
            )
        self.run(cmd=cmd)

    def configure_external_host(self):
        print('***** add external host on {} *****'.format(self.container))
        gw_ip = netaddr.IPAddress(self.ext_net.last - 1)
        host_ip = netaddr.IPAddress(self.ext_net.last - 2)

        self.run(cmd='ip link add veth0 type veth peer name veth1')
        self.run(cmd='ip link add veth0 type veth peer name veth1')
        self.run(cmd='ip netns add ext-ns')
        self.run(cmd='ip link set netns ext-ns dev veth0')
        self.run(cmd='ip netns exec ext-ns ip link set dev veth0 up')
        self.run(
            cmd='ip netns exec ext-ns ip addr add {}/{} dev veth0'.format(
                host_ip, self.ext_net.prefixlen))
        self.run(
            cmd='ip netns exec ext-ns ip route add default via {}'.format(
                gw_ip))
        self.run(cmd='ip link set dev veth1 up')
        self.run(cmd='ovs-vsctl add-port br-ex veth1')

    def configure(self, physical_net):
        self.configure_localnet(physical_net)
        self.configure_external_host()

    @ovn_stats.timeit
    def wait(self, sbctl, timeout_s):
        for _ in range(timeout_s * 10):
            if sbctl.chassis_bound(self.container):
                return
            time.sleep(0.1)
        raise ovn_utils.OvnChassisTimeoutException()

    @ovn_stats.timeit
    def provision(self, cluster):
        self.connect(cluster.cluster_cfg)
        self.wait(cluster.sbctl, cluster.cluster_cfg.node_timeout_s)

        # Create a node switch and connect it to the cluster router.
        self.switch = cluster.nbctl.ls_add('lswitch-{}'.format(self.container),
                                           cidr=self.int_net)
        lrp_name = 'rtr-to-node-{}'.format(self.container)
        ls_rp_name = 'node-to-rtr-{}'.format(self.container)
        lrp_ip = netaddr.IPAddress(self.int_net.last - 1)
        self.rp = cluster.nbctl.lr_port_add(
            cluster.router, lrp_name, RandMac(), lrp_ip,
            self.int_net.prefixlen
        )
        self.ls_rp = cluster.nbctl.ls_port_add(
            self.switch, ls_rp_name, self.rp
        )

        # Create a join switch and connect it to the cluster router.
        self.join_switch = cluster.nbctl.ls_add(
            'join-{}'.format(self.container), cidr=self.gw_net
        )
        join_lrp_name = 'rtr-to-join-{}'.format(self.container)
        join_ls_rp_name = 'join-to-rtr-{}'.format(self.container)
        lrp_ip = netaddr.IPAddress(self.gw_net.last - 1)
        self.join_rp = cluster.nbctl.lr_port_add(
            cluster.router, join_lrp_name, RandMac(), lrp_ip,
            self.gw_net.prefixlen
        )
        self.join_ls_rp = cluster.nbctl.ls_port_add(
            self.join_switch, join_ls_rp_name, self.join_rp
        )

        # Create a gw router and connect it to the join switch.
        self.gw_router = cluster.nbctl.lr_add(
            'gwrouter-{}'.format(self.container)
        )
        cluster.nbctl.run('set Logical_Router {} options:chassis={}'.format(
            self.gw_router, self.container))
        join_grp_name = 'gw-to-join-{}'.format(self.container)
        join_ls_grp_name = 'join-to-gw-{}'.format(self.container)
        gr_gw = netaddr.IPAddress(self.gw_net.last - 2)
        self.gw_rp = cluster.nbctl.lr_port_add(
            self.gw_router, join_grp_name, RandMac(), gr_gw,
            self.gw_net.prefixlen
        )
        self.join_gw_rp = cluster.nbctl.ls_port_add(
            self.join_switch, join_ls_grp_name, self.gw_rp
        )

        # Create an external switch connecting the gateway router to the
        # physnet.
        self.ext_switch = cluster.nbctl.ls_add(
            'ext-{}'.format(self.container), cidr=self.ext_net
        )
        ext_lrp_name = 'gw-to-ext-{}'.format(self.container)
        ext_ls_rp_name = 'ext-to-gw-{}'.format(self.container)
        lrp_ip = netaddr.IPAddress(self.ext_net.last - 1)
        self.ext_rp = cluster.nbctl.lr_port_add(
            self.gw_router, ext_lrp_name, RandMac(), lrp_ip,
            self.ext_net.prefixlen
        )
        self.ext_gw_rp = cluster.nbctl.ls_port_add(
            self.ext_switch, ext_ls_rp_name, self.ext_rp
        )

        # Configure physnet.
        self.physnet_port = cluster.nbctl.ls_port_add(
            self.ext_switch, 'provnet-{}'.format(self.container),
            ip="unknown"
        )
        cluster.nbctl.ls_port_set_set_type(self.physnet_port, 'localnet')
        cluster.nbctl.ls_port_set_set_options(
            self.physnet_port,
            'network_name={}'.format(cluster.brex_cfg.physical_net)
        )

        # Route for traffic entering the cluster.
        rp_gw = netaddr.IPAddress(self.gw_net.last - 1)
        cluster.nbctl.route_add(self.gw_router, cluster.net, str(rp_gw))

        # Default route to get out of cluster via physnet.
        gr_def_gw = netaddr.IPAddress(self.ext_net.last - 2)
        cluster.nbctl.route_add(self.gw_router, gw=str(gr_def_gw))

        # Force return traffic to return on the same node.
        cluster.nbctl.run(
            'set Logical_Router {} options:lb_force_snat_ip={}'.format(
                self.gw_router, str(gr_gw)
            )
        )

        # Route for traffic that needs to exit the cluster
        # (via gw router).
        cluster.nbctl.route_add(cluster.router, str(self.int_net),
                                str(gr_gw), policy="src-ip")

        # SNAT traffic leaving the cluster.
        cluster.nbctl.nat_add(self.gw_router, external_ip=str(gr_gw),
                              logical_ip=cluster.net)

    @ovn_stats.timeit
    def provision_port(self, cluster):
        name = 'lp-{}-{}'.format(self.id, len(self.lports))
        ip = netaddr.IPAddress(self.int_net.first + len(self.lports) + 1)
        plen = self.int_net.prefixlen
        gw = netaddr.IPAddress(self.int_net.last - 1)
        ext_gw = netaddr.IPAddress(self.ext_net.last - 2)

        print("***** creating lport {} *****".format(name))
        lport = cluster.nbctl.ls_port_add(self.switch, name,
                                          mac=str(RandMac()), ip=ip, plen=plen,
                                          gw=gw, ext_gw=ext_gw, metadata=self)
        self.lports.append(lport)
        return lport

    @ovn_stats.timeit
    def bind_port(self, port):
        vsctl = ovn_utils.OvsVsctl(self)
        vsctl.add_port(port, 'br-int', internal=True, ifaceid=port['name'])
        vsctl.bind_vm_port(port)

    def provision_ports(self, cluster, n_ports):
        ports = [self.provision_port(cluster) for i in range(n_ports)]
        for port in ports:
            self.bind_port(port)
        return ports

    def run_ping(self, cluster, src, dest):
        print(f'***** pinging from {src} to {dest} *****')
        cmd = f'ip netns exec {src} ping -q -c 1 -W 0.1 {dest}'
        start_time = datetime.now()
        while True:
            try:
                self.run(cmd=cmd, raise_on_error=True)
                break
            except ovn_utils.SSHError:
                pass

            duration = (datetime.now() - start_time).seconds
            if (duration > cluster.cluster_cfg.node_timeout_s):
                print(f'***** Error: Timeout waiting for {src} '
                      f'to be able to ping {dest} *****')
                raise ovn_utils.OvnPingTimeoutException()

    @ovn_stats.timeit
    def ping_port(self, cluster, port, dest=None):
        if not dest:
            dest = port['ext-gw']
        self.run_ping(cluster, port['name'], dest)

    @ovn_stats.timeit
    def ping_external(self, cluster, port):
        self.run_ping(cluster, 'ext-ns', port['ip'])

    def ping_ports(self, cluster, ports):
        for port in ports:
            self.ping_port(cluster, port)


ACL_DEFAULT_DENY_PRIO = 1
ACL_DEFAULT_ALLOW_ARP_PRIO = 2
ACL_NETPOL_ALLOW_PRIO = 3


class Namespace(object):
    def __init__(self, cluster, name):
        self.cluster = cluster
        self.nbctl = cluster.nbctl
        self.lports = []
        self.pg_def_deny_igr = \
            self.nbctl.port_group_create(f'pg_deny_igr_{name}')
        self.pg_def_deny_egr = \
            self.nbctl.port_group_create(f'pg_deny_egr_{name}')
        self.pg = self.nbctl.port_group_create(f'pg_{name}')
        self.addr_set = self.nbctl.address_set_create(f'as_{name}')

        # Default policies.
        self.nbctl.acl_add(
            self.pg_def_deny_igr['name'],
            'to-lport', ACL_DEFAULT_DENY_PRIO, 'port-group',
            f'ip4.src == \\${self.addr_set["name"]} && '
            f'outport == @{self.pg_def_deny_igr["name"]}',
            'drop')
        self.nbctl.acl_add(
            self.pg_def_deny_egr['name'],
            'to-lport', ACL_DEFAULT_DENY_PRIO, 'port-group',
            f'ip4.dst == \\${self.addr_set["name"]} && '
            f'inport == @{self.pg_def_deny_egr["name"]}',
            'drop')
        self.nbctl.acl_add(
            self.pg_def_deny_igr['name'],
            'to-lport', ACL_DEFAULT_ALLOW_ARP_PRIO, 'port-group',
            f'outport == @{self.pg_def_deny_igr["name"]} && arp',
            'allow')
        self.nbctl.acl_add(
            self.pg_def_deny_egr['name'],
            'to-lport', ACL_DEFAULT_ALLOW_ARP_PRIO, 'port-group',
            f'inport == @{self.pg_def_deny_egr["name"]} && arp',
            'allow')

    @ovn_stats.timeit
    def add_port(self, port):
        self.lports.append(port)
        self.nbctl.port_group_add(self.pg_def_deny_igr, port)
        self.nbctl.port_group_add(self.pg_def_deny_egr, port)
        self.nbctl.port_group_add(self.pg, port)
        if port.get('ip'):
            self.nbctl.address_set_add(self.addr_set, str(port['ip']))

    @ovn_stats.timeit
    def allow_within_namespace(self):
        self.nbctl.acl_add(
            self.pg['name'], 'to-lport', ACL_NETPOL_ALLOW_PRIO, 'port-group',
            f'ip4.src == \\${self.addr_set["name"]} && '
            f'outport == @{self.pg["name"]}',
            'allow-related'
        )
        self.nbctl.acl_add(
            self.pg['name'], 'to-lport', ACL_NETPOL_ALLOW_PRIO, 'port-group',
            f'ip4.dst == \\${self.addr_set["name"]} && '
            f'inport == @{self.pg["name"]}',
            'allow-related'
        )

    @ovn_stats.timeit
    def allow_from_external(self, external_ips, include_ext_gw=False):
        # If requested, include the ext-gw of the first port in the namespace
        # so we can check that this rule is enforced.
        if include_ext_gw:
            assert(len(self.lports) > 0)
            external_ips.append(self.lports[0]['ext-gw'])
        ips = [str(ip) for ip in external_ips]
        self.nbctl.acl_add(
            self.pg['name'], 'to-lport', ACL_NETPOL_ALLOW_PRIO, 'port-group',
            f'ip4.src == {{{",".join(ips)}}} && outport == @{self.pg["name"]}',
            'allow-related'
        )

    @ovn_stats.timeit
    def check_enforcing_internal(self):
        # "Random" check that first pod can reach last pod in the namespace.
        if len(self.lports) > 1:
            src = self.lports[0]
            dst = self.lports[-1]
            worker = src['metadata']
            worker.ping_port(self.cluster, src, dst['ip'])

    @ovn_stats.timeit
    def check_enforcing_external(self):
        if len(self.lports) > 0:
            dst = self.lports[0]
            worker = dst['metadata']
            worker.ping_external(self.cluster, dst)


class Cluster(object):
    def __init__(self, central_node, worker_nodes, cluster_cfg, brex_cfg):
        # In clustered mode use the first node for provisioning.
        self.central_node = central_node
        self.worker_nodes = worker_nodes
        self.cluster_cfg = cluster_cfg
        self.brex_cfg = brex_cfg
        self.nbctl = ovn_utils.OvnNbctl(self.central_node)
        self.sbctl = ovn_utils.OvnSbctl(self.central_node)
        self.net = cluster_cfg.cluster_net
        self.router = None
        self.load_balancer = None
        self.last_selected_worker = 0

    def start(self):
        self.central_node.start(self.cluster_cfg)
        for w in self.worker_nodes:
            w.start(self.cluster_cfg)
            w.configure(self.brex_cfg.physical_net)
        self.nbctl.start_daemon()
        self.nbctl.set_global(
            'use_logical_dp_groups',
            self.cluster_cfg.logical_dp_groups
        )

    def create_cluster_router(self, rtr_name):
        self.router = self.nbctl.lr_add(rtr_name)

    def create_cluster_load_balancer(self, lb_name):
        self.load_balancer = lb.OvnLoadBalancer(lb_name, self.nbctl,
                                                self.cluster_cfg.vips)
        self.load_balancer.add_vips(self.cluster_cfg.static_vips)

    def select_worker_for_port(self):
        self.last_selected_worker += 1
        self.last_selected_worker %= len(self.worker_nodes)
        return self.worker_nodes[self.last_selected_worker]
