import logging
from collections import namedtuple

import netaddr

from randmac import RandMac

import ovn_load_balancer as lb
import ovn_utils
import ovn_stats

from ovn_utils import DualStackSubnet
from ovn_workload import ChassisNode, Cluster

log = logging.getLogger(__name__)
ClusterBringupCfg = namedtuple('ClusterBringupCfg', ['n_pods_per_node'])
OVN_HEATER_CMS_PLUGIN = 'OVNKubernetesCluster'
ACL_DEFAULT_DENY_PRIO = 1
ACL_DEFAULT_ALLOW_ARP_PRIO = 2
ACL_NETPOL_ALLOW_PRIO = 3
DEFAULT_NS_VIP_SUBNET = netaddr.IPNetwork('30.0.0.0/16')
DEFAULT_NS_VIP_SUBNET6 = netaddr.IPNetwork('30::/32')
DEFAULT_VIP_PORT = 80
DEFAULT_BACKEND_PORT = 8080


class Namespace:
    def __init__(self, clusters, name, global_cfg):
        self.clusters = clusters
        self.nbctl = [cluster.nbctl for cluster in clusters]
        self.ports = [[] for _ in range(len(clusters))]
        self.enforcing = False
        self.pg_def_deny_igr = [
            nbctl.port_group_create(f'pg_deny_igr_{name}')
            for nbctl in self.nbctl
        ]
        self.pg_def_deny_egr = [
            nbctl.port_group_create(f'pg_deny_egr_{name}')
            for nbctl in self.nbctl
        ]
        self.pg = [
            nbctl.port_group_create(f'pg_{name}') for nbctl in self.nbctl
        ]
        self.addr_set4 = [
            (
                nbctl.address_set_create(f'as_{name}')
                if global_cfg.run_ipv4
                else None
            )
            for nbctl in self.nbctl
        ]
        self.addr_set6 = [
            (
                nbctl.address_set_create(f'as6_{name}')
                if global_cfg.run_ipv6
                else None
            )
            for nbctl in self.nbctl
        ]
        self.sub_as = [[] for _ in range(len(clusters))]
        self.sub_pg = [[] for _ in range(len(clusters))]
        self.load_balancer = None
        for cluster in self.clusters:
            cluster.n_ns += 1
        self.name = name

    @ovn_stats.timeit
    def add_ports(self, ports, az=0):
        self.ports[az].extend(ports)
        # Always add port IPs to the address set but not to the PGs.
        # Simulate what OpenShift does, which is: create the port groups
        # when the first network policy is applied.
        if self.addr_set4:
            for i, nbctl in enumerate(self.nbctl):
                nbctl.address_set_add_addrs(
                    self.addr_set4[i], [str(p.ip) for p in ports]
                )
        if self.addr_set6:
            for i, nbctl in enumerate(self.nbctl):
                nbctl.address_set_add_addrs(
                    self.addr_set6[i], [str(p.ip6) for p in ports]
                )
        if self.enforcing:
            self.nbctl[az].port_group_add_ports(
                self.pg_def_deny_igr[az], ports
            )
            self.nbctl[az].port_group_add_ports(
                self.pg_def_deny_egr[az], ports
            )
            self.nbctl[az].port_group_add_ports(self.pg[az], ports)

    def unprovision(self):
        # ACLs are garbage collected by OVSDB as soon as all the records
        # referencing them are removed.
        for i, cluster in enumerate(self.clusters):
            cluster.unprovision_ports(self.ports[i])
        for i, nbctl in enumerate(self.nbctl):
            nbctl.port_group_del(self.pg_def_deny_igr[i])
            nbctl.port_group_del(self.pg_def_deny_egr[i])
            nbctl.port_group_del(self.pg[i])
            if self.addr_set4:
                nbctl.address_set_del(self.addr_set4[i])
            if self.addr_set6:
                nbctl.address_set_del(self.addr_set6[i])
            nbctl.port_group_del(self.sub_pg[i])
            nbctl.address_set_del(self.sub_as[i])

    def unprovision_ports(self, ports, az=0):
        '''Unprovision a subset of ports in the namespace without having to
        unprovision the entire namespace or any of its network policies.'''

        for port in ports:
            self.ports[az].remove(port)

        self.clusters[az].unprovision_ports(ports)

    def enforce(self):
        if self.enforcing:
            return
        self.enforcing = True
        for i, nbctl in enumerate(self.nbctl):
            nbctl.port_group_add_ports(self.pg_def_deny_igr[i], self.ports[i])
            nbctl.port_group_add_ports(self.pg_def_deny_egr[i], self.ports[i])
            nbctl.port_group_add_ports(self.pg[i], self.ports[i])

    def create_sub_ns(self, ports, global_cfg, az=0):
        n_sub_pgs = len(self.sub_pg[az])
        suffix = f'{self.name}_{n_sub_pgs}'
        pg = self.nbctl[az].port_group_create(f'sub_pg_{suffix}')
        self.nbctl[az].port_group_add_ports(pg, ports)
        self.sub_pg[az].append(pg)
        for i, nbctl in enumerate(self.nbctl):
            if global_cfg.run_ipv4:
                addr_set = nbctl.address_set_create(f'sub_as_{suffix}')
                nbctl.address_set_add_addrs(
                    addr_set, [str(p.ip) for p in ports]
                )
                self.sub_as[i].append(addr_set)
            if global_cfg.run_ipv6:
                addr_set = nbctl.address_set_create(f'sub_as_{suffix}6')
                nbctl.address_set_add_addrs(
                    addr_set, [str(p.ip6) for p in ports]
                )
                self.sub_as[i].append(addr_set)
        return n_sub_pgs

    @ovn_stats.timeit
    def default_deny(self, family, az=0):
        self.enforce()

        addr_set = f'self.addr_set{family}.name'
        self.nbctl[az].acl_add(
            self.pg_def_deny_igr[az].name,
            'to-lport',
            ACL_DEFAULT_DENY_PRIO,
            'port-group',
            f'ip4.src == \\${addr_set} && '
            f'outport == @{self.pg_def_deny_igr[az].name}',
            'drop',
        )
        self.nbctl[az].acl_add(
            self.pg_def_deny_egr[az].name,
            'to-lport',
            ACL_DEFAULT_DENY_PRIO,
            'port-group',
            f'ip4.dst == \\${addr_set} && '
            f'inport == @{self.pg_def_deny_egr[az].name}',
            'drop',
        )
        self.nbctl[az].acl_add(
            self.pg_def_deny_igr[az].name,
            'to-lport',
            ACL_DEFAULT_ALLOW_ARP_PRIO,
            'port-group',
            f'outport == @{self.pg_def_deny_igr[az].name} && arp',
            'allow',
        )
        self.nbctl[az].acl_add(
            self.pg_def_deny_egr[az].name,
            'to-lport',
            ACL_DEFAULT_ALLOW_ARP_PRIO,
            'port-group',
            f'inport == @{self.pg_def_deny_egr[az].name} && arp',
            'allow',
        )

    @ovn_stats.timeit
    def allow_within_namespace(self, family, az=0):
        self.enforce()

        addr_set = f'self.addr_set{family}.name'
        self.nbctl[az].acl_add(
            self.pg[az].name,
            'to-lport',
            ACL_NETPOL_ALLOW_PRIO,
            'port-group',
            f'ip4.src == \\${addr_set} && ' f'outport == @{self.pg[az].name}',
            'allow-related',
        )
        self.nbctl[az].acl_add(
            self.pg[az].name,
            'to-lport',
            ACL_NETPOL_ALLOW_PRIO,
            'port-group',
            f'ip4.dst == \\${addr_set} && ' f'inport == @{self.pg[az].name}',
            'allow-related',
        )

    @ovn_stats.timeit
    def allow_cross_namespace(self, ns, family):
        self.enforce()

        for az, nbctl in enumerate(self.nbctl):
            if len(self.ports[az]) == 0:
                continue
            addr_set = f'self.addr_set{family}.name'
            nbctl[az].acl_add(
                self.pg[az].name,
                'to-lport',
                ACL_NETPOL_ALLOW_PRIO,
                'port-group',
                f'ip4.src == \\${addr_set} && '
                f'outport == @{ns.pg[az].name}',
                'allow-related',
            )
            ns_addr_set = f'ns.addr_set{family}.name'
            nbctl[az].acl_add(
                self.pg[az].name,
                'to-lport',
                ACL_NETPOL_ALLOW_PRIO,
                'port-group',
                f'ip4.dst == \\${ns_addr_set} && '
                f'inport == @{self.pg[az].name}',
                'allow-related',
            )

    @ovn_stats.timeit
    def allow_sub_namespace(self, src, dst, family, az=0):
        self.nbctl[az].acl_add(
            self.pg[az].name,
            'to-lport',
            ACL_NETPOL_ALLOW_PRIO,
            'port-group',
            f'ip{family}.src == \\${self.sub_as[az][src].name} && '
            f'outport == @{self.sub_pg[az][dst].name}',
            'allow-related',
        )
        self.nbctl[az].acl_add(
            self.pg[az].name,
            'to-lport',
            ACL_NETPOL_ALLOW_PRIO,
            'port-group',
            f'ip{family}.dst == \\${self.sub_as[az][dst].name} && '
            f'inport == @{self.sub_pg[az][src].name}',
            'allow-related',
        )

    @ovn_stats.timeit
    def allow_from_external(
        self, external_ips, include_ext_gw=False, family=4, az=0
    ):
        self.enforce()
        # If requested, include the ext-gw of the first port in the namespace
        # so we can check that this rule is enforced.
        if include_ext_gw:
            assert len(self.ports) > 0
            if family == 4 and self.ports[az][0].ext_gw:
                external_ips.append(self.ports[az][0].ext_gw)
            elif family == 6 and self.ports[az][0].ext_gw6:
                external_ips.append(self.ports[az][0].ext_gw6)
        ips = [str(ip) for ip in external_ips]
        self.nbctl[az].acl_add(
            self.pg[az].name,
            'to-lport',
            ACL_NETPOL_ALLOW_PRIO,
            'port-group',
            f'ip.{family} == {{{",".join(ips)}}} && '
            f'outport == @{self.pg[az].name}',
            'allow-related',
        )

    @ovn_stats.timeit
    def check_enforcing_internal(self, az=0):
        # "Random" check that first pod can reach last pod in the namespace.
        if len(self.ports[az]) > 1:
            src = self.ports[az][0]
            dst = self.ports[az][-1]
            worker = src.metadata
            if src.ip:
                worker.ping_port(self.clusters[az], src, dst.ip)
            if src.ip6:
                worker.ping_port(self.clusters[az], src, dst.ip6)

    @ovn_stats.timeit
    def check_enforcing_external(self, az=0):
        if len(self.ports[az]) > 0:
            dst = self.ports[az][0]
            worker = dst.metadata
            worker.ping_external(self.clusters[az], dst)

    @ovn_stats.timeit
    def check_enforcing_cross_ns(self, ns, az=0):
        if len(self.ports[az]) > 0 and len(ns.ports[az]) > 0:
            dst = ns.ports[az][0]
            src = self.ports[az][0]
            worker = src.metadata
            if src.ip and dst.ip:
                worker.ping_port(self.clusters[az], src, dst.ip)
            if src.ip6 and dst.ip6:
                worker.ping_port(self.clusters[az], src, dst.ip6)

    def create_load_balancer(self, az=0):
        self.load_balancer = lb.OvnLoadBalancer(
            f'lb_{self.name}', self.nbctl[az]
        )

    @ovn_stats.timeit
    def provision_vips_to_load_balancers(self, backend_lists, version, az=0):
        vip_ns_subnet = DEFAULT_NS_VIP_SUBNET
        if version == 6:
            vip_ns_subnet = DEFAULT_NS_VIP_SUBNET6
        vip_net = vip_ns_subnet.next(self.clusters[az].n_ns)
        n_vips = len(self.load_balancer.vips.keys())
        vip_ip = vip_net.ip.__add__(n_vips + 1)

        if version == 6:
            vips = {
                f'[{vip_ip + i}]:{DEFAULT_VIP_PORT}': [
                    f'[{p.ip6}]:{DEFAULT_BACKEND_PORT}' for p in ports
                ]
                for i, ports in enumerate(backend_lists)
            }
            self.load_balancer.add_vips(vips)
        else:
            vips = {
                f'{vip_ip + i}:{DEFAULT_VIP_PORT}': [
                    f'{p.ip}:{DEFAULT_BACKEND_PORT}' for p in ports
                ]
                for i, ports in enumerate(backend_lists)
            }
            self.load_balancer.add_vips(vips)


class OVNKubernetesCluster(Cluster):
    def __init__(self, cluster_cfg, central, brex_cfg, az):
        super().__init__(cluster_cfg, central, brex_cfg, az)
        self.net = cluster_cfg.cluster_net
        self.gw_net = ovn_utils.DualStackSubnet.next(
            cluster_cfg.gw_net,
            az * (cluster_cfg.n_workers // cluster_cfg.n_az),
        )
        self.router = None
        self.load_balancer = None
        self.load_balancer6 = None
        self.join_switch = None
        self.last_selected_worker = 0
        self.n_ns = 0
        self.ts_switch = None

    def add_cluster_worker_nodes(self, workers):
        cluster_cfg = self.cluster_cfg

        # Allocate worker IPs after central and relay IPs.
        mgmt_ip = (
            cluster_cfg.node_net.ip
            + 2
            + cluster_cfg.n_az
            * (len(self.central_nodes) + len(self.relay_nodes))
        )

        protocol = "ssl" if cluster_cfg.enable_ssl else "tcp"
        internal_net = cluster_cfg.internal_net
        external_net = cluster_cfg.external_net
        # Number of workers for each az
        n_az_workers = cluster_cfg.n_workers // cluster_cfg.n_az
        self.add_workers(
            [
                WorkerNode(
                    workers[i % len(workers)],
                    f'ovn-scale-{i}',
                    mgmt_ip + i,
                    protocol,
                    DualStackSubnet.next(internal_net, i),
                    DualStackSubnet.next(external_net, i),
                    self.gw_net,
                    i,
                )
                for i in range(self.az * n_az_workers, (self.az + 1) * n_az_workers)
            ]
        )

    def create_cluster_router(self, rtr_name):
        self.router = self.nbctl.lr_add(rtr_name)
        self.nbctl.lr_set_options(
            self.router,
            {
                'always_learn_from_arp_request': 'false',
            },
        )

    def create_cluster_load_balancer(self, lb_name, global_cfg):
        if global_cfg.run_ipv4:
            self.load_balancer = lb.OvnLoadBalancer(
                lb_name, self.nbctl, self.cluster_cfg.vips
            )
            self.load_balancer.add_vips(self.cluster_cfg.static_vips)

        if global_cfg.run_ipv6:
            self.load_balancer6 = lb.OvnLoadBalancer(
                f'{lb_name}6', self.nbctl, self.cluster_cfg.vips6
            )
            self.load_balancer6.add_vips(self.cluster_cfg.static_vips6)

    def create_cluster_join_switch(self, sw_name):
        self.join_switch = self.nbctl.ls_add(sw_name, net_s=self.gw_net)

        self.join_rp = self.nbctl.lr_port_add(
            self.router,
            f'rtr-to-{sw_name}',
            RandMac(),
            self.gw_net.reverse(),
        )
        self.join_ls_rp = self.nbctl.ls_port_add(
            self.join_switch, f'{sw_name}-to-rtr', self.join_rp
        )

    @ovn_stats.timeit
    def provision_vips_to_load_balancers(self, backend_lists):
        n_vips = len(self.load_balancer.vips.keys())
        vip_ip = self.cluster_cfg.vip_subnet.ip.__add__(n_vips + 1)

        vips = {
            f'{vip_ip + i}:{DEFAULT_VIP_PORT}': [
                f'{p.ip}:{DEFAULT_BACKEND_PORT}' for p in ports
            ]
            for i, ports in enumerate(backend_lists)
        }
        self.load_balancer.add_vips(vips)

    def unprovision_vips(self):
        if self.load_balancer:
            self.load_balancer.clear_vips()
            self.load_balancer.add_vips(self.cluster_cfg.static_vips)
        if self.load_balancer6:
            self.load_balancer6.clear_vips()
            self.load_balancer6.add_vips(self.cluster_cfg.static_vips6)

    def provision_lb_group(self, name='cluster-lb-group'):
        self.lb_group = lb.OvnLoadBalancerGroup(name, self.nbctl)
        for w in self.worker_nodes:
            self.nbctl.ls_add_lbg(w.switch, self.lb_group.lbg)
            self.nbctl.lr_add_lbg(w.gw_router, self.lb_group.lbg)

    def provision_lb(self, lb):
        log.info(f'Creating load balancer {lb.name}')
        self.lb_group.add_lb(lb)


class WorkerNode(ChassisNode):
    def __init__(
        self,
        phys_node,
        container,
        mgmt_ip,
        protocol,
        int_net,
        ext_net,
        gw_net,
        unique_id,
    ):
        super().__init__(phys_node, container, mgmt_ip, protocol)
        self.int_net = int_net
        self.ext_net = ext_net
        self.gw_net = gw_net
        self.id = unique_id

    def configure(self, physical_net):
        self.configure_localnet(physical_net)
        phys_ctl = ovn_utils.PhysCtl(self)
        phys_ctl.external_host_provision(
            ip=self.ext_net.reverse(2), gw=self.ext_net.reverse()
        )

    @ovn_stats.timeit
    def provision(self, cluster):
        self.connect(cluster.get_relay_connection_string())
        self.wait(cluster.sbctl, cluster.cluster_cfg.node_timeout_s)

        # Create a node switch and connect it to the cluster router.
        self.switch = cluster.nbctl.ls_add(
            f'lswitch-{self.container}', net_s=self.int_net
        )
        lrp_name = f'rtr-to-node-{self.container}'
        ls_rp_name = f'node-to-rtr-{self.container}'
        self.rp = cluster.nbctl.lr_port_add(
            cluster.router, lrp_name, RandMac(), self.int_net.reverse()
        )
        self.ls_rp = cluster.nbctl.ls_port_add(
            self.switch, ls_rp_name, self.rp
        )

        # Make the lrp as distributed gateway router port.
        cluster.nbctl.lr_port_set_gw_chassis(self.rp, self.container)

        # Create a gw router and connect it to the cluster join switch.
        self.gw_router = cluster.nbctl.lr_add(f'gwrouter-{self.container}')
        cluster.nbctl.lr_set_options(
            self.gw_router,
            {
                'always_learn_from_arp_request': 'false',
                'dynamic_neigh_routers': 'true',
                'chassis': self.container,
                'lb_force_snat_ip': 'router_ip',
                'snat-ct-zone': 0,
            },
        )
        join_grp_name = f'gw-to-join-{self.container}'
        join_ls_grp_name = f'join-to-gw-{self.container}'

        gr_gw = self.gw_net.reverse(self.id + 2)
        self.gw_rp = cluster.nbctl.lr_port_add(
            self.gw_router, join_grp_name, RandMac(), gr_gw
        )
        self.join_gw_rp = cluster.nbctl.ls_port_add(
            cluster.join_switch, join_ls_grp_name, self.gw_rp
        )

        # Create an external switch connecting the gateway router to the
        # physnet.
        self.ext_switch = cluster.nbctl.ls_add(
            f'ext-{self.container}', net_s=self.ext_net
        )
        ext_lrp_name = f'gw-to-ext-{self.container}'
        ext_ls_rp_name = f'ext-to-gw-{self.container}'
        self.ext_rp = cluster.nbctl.lr_port_add(
            self.gw_router, ext_lrp_name, RandMac(), self.ext_net.reverse()
        )
        self.ext_gw_rp = cluster.nbctl.ls_port_add(
            self.ext_switch, ext_ls_rp_name, self.ext_rp
        )

        # Configure physnet.
        self.physnet_port = cluster.nbctl.ls_port_add(
            self.ext_switch,
            f'provnet-{self.container}',
            localnet=True,
        )
        cluster.nbctl.ls_port_set_set_type(self.physnet_port, 'localnet')
        cluster.nbctl.ls_port_set_set_options(
            self.physnet_port, f'network_name={cluster.brex_cfg.physical_net}'
        )

        # Route for traffic entering the cluster.
        cluster.nbctl.route_add(
            self.gw_router, cluster.net, self.gw_net.reverse()
        )

        # Default route to get out of cluster via physnet.
        cluster.nbctl.route_add(
            self.gw_router,
            ovn_utils.DualStackSubnet(
                netaddr.IPNetwork("0.0.0.0/0"), netaddr.IPNetwork("::/0")
            ),
            self.ext_net.reverse(2),
        )

        # Route for traffic that needs to exit the cluster
        # (via gw router).
        cluster.nbctl.route_add(
            cluster.router, self.int_net, gr_gw, policy="src-ip"
        )

        # SNAT traffic leaving the cluster.
        cluster.nbctl.nat_add(self.gw_router, gr_gw, cluster.net)

    @ovn_stats.timeit
    def provision_port(self, cluster, passive=False):
        name = f'lp-{self.id}-{self.next_lport_index}'

        log.info(f'Creating lport {name}')
        lport = cluster.nbctl.ls_port_add(
            self.switch,
            name,
            mac=str(RandMac()),
            ip=self.int_net.forward(self.next_lport_index + 1),
            gw=self.int_net.reverse(),
            ext_gw=self.ext_net.reverse(2),
            metadata=self,
            passive=passive,
            security=True,
        )

        self.lports.append(lport)
        self.next_lport_index += 1
        return lport

    @ovn_stats.timeit
    def provision_load_balancers(self, cluster, ports, global_cfg):
        # Add one port IP as a backend to the cluster load balancer.
        if global_cfg.run_ipv4:
            port_ips = (
                f'{port.ip}:{DEFAULT_BACKEND_PORT}'
                for port in ports
                if port.ip is not None
            )
            cluster_vips = cluster.cluster_cfg.vips.keys()
            cluster.load_balancer.add_backends_to_vip(port_ips, cluster_vips)
            cluster.load_balancer.add_to_switches([self.switch.name])
            cluster.load_balancer.add_to_routers([self.gw_router.name])

        if global_cfg.run_ipv6:
            port_ips6 = (
                f'[{port.ip6}]:{DEFAULT_BACKEND_PORT}'
                for port in ports
                if port.ip6 is not None
            )
            cluster_vips6 = cluster.cluster_cfg.vips6.keys()
            cluster.load_balancer6.add_backends_to_vip(
                port_ips6, cluster_vips6
            )
            cluster.load_balancer6.add_to_switches([self.switch.name])
            cluster.load_balancer6.add_to_routers([self.gw_router.name])

        # GW Load balancer has no VIPs/backends configured on it, since
        # this load balancer is used for hostnetwork services. We're not
        # using those right now so the load blaancer is empty.
        if global_cfg.run_ipv4:
            self.gw_load_balancer = lb.OvnLoadBalancer(
                f'lb-{self.gw_router.name}', cluster.nbctl
            )
            self.gw_load_balancer.add_to_routers([self.gw_router.name])
        if global_cfg.run_ipv6:
            self.gw_load_balancer6 = lb.OvnLoadBalancer(
                f'lb-{self.gw_router.name}6', cluster.nbctl
            )
            self.gw_load_balancer6.add_to_routers([self.gw_router.name])

    @ovn_stats.timeit
    def ping_external(self, cluster, port):
        if port.ip:
            self.run_ping(cluster, 'ext-ns', port.ip)
        if port.ip6:
            self.run_ping(cluster, 'ext-ns', port.ip6)
