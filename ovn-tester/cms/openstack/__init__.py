# Copyright 2023 Canonical
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
from collections import namedtuple

import netaddr

from randmac import RandMac

import ovn_load_balancer as lb
import ovn_utils
import ovn_stats

from ovn_context import Context
from ovn_utils import DualStackSubnet
from ovn_workload import ChassisNode, Cluster

log = logging.getLogger(__name__)


class OpenStackCloud(Cluster):
    def __init__(self, central_node, worker_nodes, cluster_cfg, brex_cfg):
        super().__init__(
            central_node, worker_nodes, cluster_cfg, brex_cfg
        )
        self.net = cluster_cfg.cluster_net
        self.router = None


class Project(object):
    """Represent network components of an OpenStack Project aka. Tenant."""
    def __init__(self, cluster, name, global_cfg):
        self.cluster = cluster

    def create_project_router(self, rtr_name):
        self.router = self.nbctl.lr_add(rtr_name)
        self.nbctl.lr_set_options(
            self.router,
            {
                "always_learn_from_arp_request": "false",
            },
        )


class NetworkNode(ChassisNode):
    """Represent a network node, a node with OVS/OVN but no VMs."""
    def __init__(
        self,
        phys_node,
        container,
        mgmt_net,
        mgmt_ip
    ):
        super().__init__(phys_node, container, mgmt_net, mgmt_ip)

    def configure(self, physical_net):
        pass


class ComputeNode(ChassisNode):
    """Represent a compute node, a node with OVS/OVN as well as VMs."""
    def __init__(
        self,
        phys_node,
        container,
        mgmt_net,
        mgmt_ip,
    ):
        super().__init__(phys_node, container, mgmt_net, mgmt_ip)

    def configure(self, physical_net):
        pass


class openstack(object):
    def __init__(self, cms_config):
        self.read_config(cms_config)

    def read_config(self, cms_config):
        pass

    def create_nodes(self, cluster_config, workers):
        mgmt_net = cluster_config.node_net
        mgmt_ip = mgmt_net.ip + 2
        network_nodes = [
            NetworkNode(
                workers[i % len(workers)],
                f"ovn-scale-network-{i}",
                mgmt_net,
                mgmt_ip + i,
            )
            for i in range(cluster_config.n_workers)
        ]
        compute_nodes = [
            ComputeNode(
                workers[i % len(workers)],
                f"ovn-scale-compute-{i}",
                mgmt_net,
                mgmt_ip + i,
            )
            for i in range(cluster_config.n_workers)
        ]
        return network_nodes + compute_nodes

    def prepare_test(self, central_node, worker_nodes, cluster_cfg, brex_cfg):
        ovn = OpenStackCloud(
            central_node, worker_nodes, cluster_cfg, brex_cfg
        )
        with Context(ovn, "prepare_test"):
            ovn.start()
        return ovn

    def run_base_cluster_bringup(self, ovn, global_cfg):
        # create ovn topology
        with Context(
            ovn, "base_cluster_bringup", len(ovn.worker_nodes)
        ) as ctx:
            for i in ctx:
                log.info(f"HELLO! {i}")
