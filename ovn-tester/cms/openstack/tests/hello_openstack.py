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

from ovn_ext_cmd import ExtCmd

log = logging.getLogger(__name__)


class HelloOpenstack(ExtCmd):
    def __init__(self, config, central_node, worker_nodes, global_cfg):
        super().__init__(config, central_node, worker_nodes)
        log.info("HELLO __init__")

    def run(self, ovn, global_cfg):
        log.info("HELLO run")
