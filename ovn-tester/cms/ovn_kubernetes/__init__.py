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

from collections import namedtuple

from ovn_context import Context
from ovn_workload import WorkerNode, Cluster
from ovn_utils import DualStackSubnet

ClusterBringupCfg = namedtuple("ClusterBringupCfg", ["n_pods_per_node"])


class ovn_kubernetes(object):
    def __init__(self, cms_config):
        self.read_config(cms_config)

    def read_config(self, cms_config):
        bringup_args = cms_config.get("base_cluster_bringup", dict())
        self.bringup_cfg = ClusterBringupCfg(
            n_pods_per_node=bringup_args.get("n_pods_per_node", 10)
        )

    def create_nodes(self, cluster_config, workers):
        mgmt_net = cluster_config.node_net
        mgmt_ip = mgmt_net.ip + 2
        internal_net = cluster_config.internal_net
        external_net = cluster_config.external_net
        gw_net = cluster_config.gw_net
        worker_nodes = [
            WorkerNode(
                workers[i % len(workers)],
                f"ovn-scale-{i}",
                mgmt_net,
                mgmt_ip + i,
                DualStackSubnet.next(internal_net, i),
                DualStackSubnet.next(external_net, i),
                gw_net,
                i,
            )
            for i in range(cluster_config.n_workers)
        ]
        return worker_nodes

    def prepare_test(self, central_node, worker_nodes, cluster_cfg, brex_cfg):
        ovn = Cluster(central_node, worker_nodes, cluster_cfg, brex_cfg)
        with Context(ovn, "prepare_test"):
            ovn.start()
        return ovn

    def run_base_cluster_bringup(self, ovn, global_cfg):
        # create ovn topology
        with Context(
            ovn, "base_cluster_bringup", len(ovn.worker_nodes)
        ) as ctx:
            ovn.create_cluster_router("lr-cluster")
            ovn.create_cluster_join_switch("ls-join")
            ovn.create_cluster_load_balancer("lb-cluster", global_cfg)
            for i in ctx:
                worker = ovn.worker_nodes[i]
                worker.provision(ovn)
                ports = worker.provision_ports(
                    ovn, self.bringup_cfg.n_pods_per_node
                )
                worker.provision_load_balancers(ovn, ports, global_cfg)
                worker.ping_ports(ovn, ports)
            ovn.provision_lb_group()
