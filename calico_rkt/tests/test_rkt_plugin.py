# Copyright 2015 Metaswitch Networks
#
# Licensed under the Apache License, Version 2.0 (the 'License');
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an 'AS IS' BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import sys
import json
import unittest
from mock import patch, Mock, call
from netaddr import IPAddress, IPNetwork
from subprocess import CalledProcessError
import pycalico.netns
import calico_rkt

CONTAINER_ID = 'ff3afbd1-17ad-499d-b514-72438c009e81'
NETNS_ROOT= '/var/lib/rkt/pods/run'
STDIN = '{"ipam": {"routes": [{"dst": "0.0.0.0/0"}], ' \
             '"subnet": "10.22.0.0/16", "type": "host-local"}, ' \
             '"type": "calico", "name": "test"}'
ORCHESTRATOR_ID = "rkt"

ENV = {
        'CNI_IFNAME': 'eth0',
        'CNI_ARGS': '',
        'CNI_COMMAND': 'ADD',
        'CNI_PATH': '/usr/lib/rkt/plugins/net:stage1/rootfs/usr/lib/rkt/plugins/net',
        'CNI_NETNS': 'netns',
        'CNI_CONTAINERID': CONTAINER_ID,
    }

class RktPluginTest(unittest.TestCase):

    @patch('calico_rkt.env', 
        autospec=True)
    @patch('sys.stdin',  
        autospec=True)
    @patch('calico_rkt.create', 
        autospec=True)
    def test_main_ADD(self, m_create, m_stdin, m_env):
        ENV['CNI_COMMAND'] = 'ADD'
        m_env.__getitem__.side_effect = lambda x: ENV[x]

        m_stdin.readlines.return_value = STDIN
        calico_rkt.main()

        m_create.assert_called_once_with(container_id=CONTAINER_ID)

    @patch('calico_rkt.env', 
        autospec=True)
    @patch('sys.stdin',  
        autospec=True)
    @patch('calico_rkt.delete', 
        autospec=True)
    def test_main_DEL(self, m_delete, m_stdin, m_env):
        ENV['CNI_COMMAND'] = 'DEL'
        m_env.__getitem__.side_effect = lambda x: ENV[x]

        m_stdin.readlines.return_value = STDIN
        calico_rkt.main()

        m_delete.assert_called_once_with(container_id=CONTAINER_ID)

    @patch('calico_rkt.INPUT_JSON', json.loads(STDIN))
    @patch('calico_rkt.env', 
        autospec=True)
    @patch('calico_rkt.IPAMClient', return_value='CLIENT',
           autospec=True)
    @patch('calico_rkt._create_calico_endpoint',
           autospec=True)
    @patch('calico_rkt._create_profile',
           autospec=True)
    def test_create(self, m_create_profile, m_create_ep, m_client, m_env):
        m_env.__getitem__.side_effect = lambda x: ENV[x]

        id_ = '123'
        ip_ = '1.2.3.4/24'
        path_ = '%s/%s/%s' % (NETNS_ROOT, id_, ENV['CNI_NETNS'])

        mock_ep = Mock()
        m_create_ep.return_value = mock_ep, ip_

        calico_rkt.create(id_)

        m_create_ep.assert_called_once_with(container_id=id_,
                                            netns_path=path_,
                                            client='CLIENT')
        m_create_profile.assert_called_once_with(endpoint=mock_ep,
                                                 profile_name="test",
                                                 ip=ip_,
                                                 client='CLIENT')
    @patch('calico_rkt.INPUT_JSON', json.loads(STDIN))
    @patch("sys.exit", 
        autospec=True)
    @patch('calico_rkt.env', 
        autospec=True)
    @patch('calico_rkt.IPAMClient', return_value='CLIENT',
           autospec=True)
    @patch('calico_rkt._create_calico_endpoint',
           autospec=True)
    @patch('calico_rkt._create_profile',
           autospec=True)
    def test_create_fail(self, m_create_profile, m_create_ep, m_client, m_env, m_sys_exit):
        m_env.__getitem__.side_effect = lambda x: ENV[x]

        id_ = '123'
        ip_ = '1.2.3.4/24'
        path_ = '%s/%s/%s' % (NETNS_ROOT, id_, ENV['CNI_NETNS'])

        mock_ep = Mock()
        m_create_ep.return_value = mock_ep, ip_
        m_create_profile.side_effect = CalledProcessError(1, "", "")

        calico_rkt.create(id_)

        m_create_ep.assert_called_once_with(container_id=id_,
                                            netns_path=path_,
                                            client='CLIENT')
        m_create_profile.assert_called_once_with(endpoint=mock_ep,
                                                 profile_name="test",
                                                 ip=ip_,
                                                 client='CLIENT')
        m_sys_exit.assert_called_once_with(1)

    @patch('calico_rkt.env', 
        autospec=True)
    @patch('calico_rkt.HOSTNAME',
        autospec=True)
    @patch('calico_rkt._container_add', return_value=('ep', 'ip'),
        autospec=True)
    def test_create_calico_endpoint(self, m_con_add, m_host, m_env):
        m_env.__getitem__.side_effect = lambda x: ENV[x]

        m_client = Mock()
        m_client.get_endpoint.return_value = None
        m_client.get_endpoint.side_effect = KeyError()

        id_, path_ = 'testcontainer', 'path/to/ns'

        calico_rkt._create_calico_endpoint(container_id=id_,
                                           netns_path=path_,
                                           client=m_client)

        m_client.get_endpoint.assert_called_once_with(hostname=m_host,
                                                      orchestrator_id=ORCHESTRATOR_ID,
                                                      workload_id=id_)
        m_con_add.assert_called_once_with(hostname=m_host,
                                          orchestrator_id=ORCHESTRATOR_ID,
                                          container_id=id_,
                                          netns_path=path_,
                                          interface=ENV['CNI_IFNAME'],
                                          client=m_client)

    @patch("sys.exit", 
        autospec=True)
    @patch('calico_rkt.env', 
        autospec=True)
    @patch('calico_rkt.HOSTNAME',
        autospec=True)
    @patch('calico_rkt._container_add', return_value=('ep', 'ip'),
        autospec=True)
    def test_create_calico_endpoint_fail(self, m_con_add, m_host, m_env, m_sys_exit):
        m_env.__getitem__.side_effect = lambda x: ENV[x]

        m_client = Mock()
        m_client.get_endpoint.return_value = "Endpoint Exists"

        id_, path_ = 'testcontainer', 'path/to/ns'

        calico_rkt._create_calico_endpoint(container_id=id_,
                                           netns_path=path_,
                                           client=m_client)

        m_client.get_endpoint.assert_called_once_with(hostname=m_host,
                                                      orchestrator_id=ORCHESTRATOR_ID,
                                                      workload_id=id_)
        m_sys_exit.assert_called_once_with(1)

    @patch('calico_rkt.HOSTNAME',
        autospec=True)
    @patch('calico_rkt._allocate_IP', return_value=IPAddress('1.2.3.4'),
        autospec=True)
    def test_container_add(self, m_allocate_IP, m_host):
        m_client = Mock()
        m_ep = Mock()
        m_client.create_endpoint.return_value = m_ep
        m_ep.provision_veth.return_value = 'macaddress'

        id_, path_ = 'testcontainer', 'path/to/ns'

        calico_rkt._container_add(hostname=m_host,
                                  orchestrator_id=ORCHESTRATOR_ID,
                                  container_id=id_,
                                  netns_path=path_,
                                  interface=ENV['CNI_IFNAME'],
                                  client=m_client)

        m_client.create_endpoint.assert_called_once_with(m_host, ORCHESTRATOR_ID, id_, [IPAddress('1.2.3.4')])
        m_ep.provision_veth.assert_called_once()
        m_client.set_endpoint.assert_called_once_with(m_ep)

    @patch('calico_rkt.INPUT_JSON', json.loads(STDIN))
    @patch('calico_rkt.HOSTNAME',
        autospec=True)
    @patch('pycalico.netns',
        autospec=True)
    def test_container_remove(self, m_netns, m_host):
        m_client = Mock()

        m_ep = Mock()
        m_ep.ipv4_nets = set()
        m_ep.ipv4_nets.add(IPNetwork('1.2.3.4/32'))
        m_ep.ipv6_nets = set()
        m_ep.name = 'endpoint_test'

        m_client.get_endpoint.return_value = m_ep
        id_ = '123'

        calico_rkt._container_remove(hostname=m_host,
                                     orchestrator_id=ORCHESTRATOR_ID,
                                     container_id=id_,
                                     client=m_client)
        m_client.get_endpoint.assert_called_once_with(hostname=m_host,
                                                      orchestrator_id=ORCHESTRATOR_ID,
                                                      workload_id=id_)
        m_client.remove_workload.assert_called_once_with(hostname=m_host,
                                                         orchestrator_id=ORCHESTRATOR_ID,
                                                         workload_id=id_)
        m_client.unassign_address.assert_called_once_with(IPNetwork("10.22.0.0/16"), IPAddress('1.2.3.4'))


    def test_create_profile(self):
        m_client = Mock()
        m_client.profile_exists.return_value = False

        m_ep = Mock()
        m_ep.endpoint_id = '1234'

        p_name, ip_ = 'profile', '1.2.3.4'

        calico_rkt._create_profile(endpoint=m_ep,
                                   profile_name=p_name,
                                   ip=ip_,
                                   client=m_client)

        m_client.profile_exists.assert_called_once_with(p_name)
        m_client.create_profile.assert_called_once_with(p_name)
        m_client.set_profiles_on_endpoint.assert_called_once_with(profile_names=[p_name], 
                                                                  endpoint_id='1234')

    @patch('calico_rkt._create_rules',
        autospec=True)
    def test_create_apply_rules(self, m_create_rules):
        m_client = Mock()
        m_profile = Mock()
        m_client.get_profile.return_value = m_profile

        p_name = 'profile'

        calico_rkt._apply_rules(profile_name=p_name,
                                client=m_client)

        m_client.profile_exists.get_profile(p_name)
        m_create_rules.assert_called_once_with(p_name)
        m_client.profile_update_rules.assert_called_once_with(m_profile)
