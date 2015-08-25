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

from __future__ import print_function
import socket
import functools
import logging
import json
import os
import sys
from subprocess import check_output, CalledProcessError, check_call
from netaddr import IPAddress, IPNetwork, AddrFormatError

from logutils import configure_logger
from pycalico import netns
from pycalico.ipam import IPAMClient, SequentialAssignment
from pycalico.netns import Namespace
from pycalico.datastore_datatypes import Rules, IPPool
from pycalico.datastore import IF_PREFIX, DatastoreClient
from pycalico.datastore_errors import PoolNotFound

logger = logging.getLogger(__name__)

ETCD_AUTHORITY_ENV = 'ETCD_AUTHORITY'

env, INPUT_JSON = {}, {}

ORCHESTRATOR_ID = "rkt"
HOSTNAME = socket.gethostname()
NETNS_ROOT = '/var/lib/rkt/pods/run'

def main():
    mode = env['CNI_COMMAND']

    if mode == 'init':
        logger.info('No initialization work to perform')
    elif mode == 'ADD':
        logger.info('Executing Calico pod-creation plugin')
        create(container_id=env['CNI_CONTAINERID'])
    elif mode == 'DEL':
        logger.info('Executing Calico pod-deletion plugin')
        delete(container_id=env['CNI_CONTAINERID'])

def create(container_id):
    """"Handle rkt pod-create event."""
    logger.info('Configuring pod %s' % container_id)
    netns_path='%s/%s/%s' % (NETNS_ROOT, container_id, env['CNI_NETNS'])
    _datastore_client = IPAMClient()

    try:
        endpoint, ip = _create_calico_endpoint(container_id=container_id,
                                               netns_path=netns_path,
                                               client=_datastore_client)

        _create_profile(endpoint=endpoint,
                        profile_name=INPUT_JSON['name'],
                        ip=ip,
                        client=_datastore_client)
    except CalledProcessError as e:
        logger.info('Error code %d creating pod networking: %s\n%s' % (
            e.returncode, e.output, e))
        sys.exit(1)
    logger.info('Finished Creating pod %s' % container_id)

def delete(container_id):
    """Cleanup after a pod."""
    logger.info('Deleting pod %s' % container_id)

    _datastore_client = IPAMClient()

    # Remove the profile for the workload.
    _container_remove(hostname=HOSTNAME,
                      orchestrator_id=ORCHESTRATOR_ID,
                      container_id=container_id,
                      client=_datastore_client)

    profile_name = INPUT_JSON['name']

    # Delete profile if only member
    if _datastore_client.profile_exists(profile_name) and \
       len(_datastore_client.get_profile_members(profile_name)) < 1:
        try:
            logger.info("Profile %s has no members, removing from datastore" % profile_name)
            _datastore_client.remove_profile(profile_name)
        except:
            logger.info("ERROR: Cannot remove profile %s; Profile cannot be found." % container_id)
            sys.exit(1)

def _create_calico_endpoint(container_id, netns_path, client):
    """
    Configure the Calico interface for a pod.
    Return Endpoint and IP
    """
    logger.info('Configuring Calico networking.')

    try:
        _ = client.get_endpoint(hostname=HOSTNAME,
                                orchestrator_id=ORCHESTRATOR_ID,
                                workload_id=container_id)
    except KeyError:
        # Calico doesn't know about this container.  Continue.
        pass
    else:
        logger.info("This container has already been configured with Calico Networking.")
        sys.exit(1)

    interface = env['CNI_IFNAME']

    endpoint, ip = _container_add(hostname=HOSTNAME,
                                  orchestrator_id=ORCHESTRATOR_ID,
                                  container_id=container_id,
                                  netns_path=netns_path,
                                  interface=interface,
                                  client=client)

    logger.info('Finished configuring network interface')
    return endpoint, ip

def _container_add(hostname, orchestrator_id, container_id, netns_path, interface, client):
    """
    Add a container to Calico networking
    Return Endpoint object and newly allocated IP
    """
    # Allocate and Assign ip address through IPAM Client
    pool = _generate_pool(client)
    ip = _allocate_IP(pool)

    # Create Endpoint object
    try:
        ep = client.create_endpoint(HOSTNAME, ORCHESTRATOR_ID,
                                      container_id, [ip])
    except AddrFormatError:
        logger.info("ERROR: This node is not configured for IPv%d. Unassigning IP "\
                      "address %s then exiting."  % ip.version, ip)
        client.unassign_address(pool, ip)
        sys.exit(1)

    # Create the veth, move into the container namespace, add the IP and
    # set up the default routes.
    ep.mac = ep.provision_veth(Namespace(netns_path), interface)
    client.set_endpoint(ep)

    return ep, ip

def _container_remove(hostname, orchestrator_id, container_id, client):
    """
    Remove the indicated container on this host from Calico networking
    """
    # Find the endpoint ID. We need this to find any ACL rules
    try:
        endpoint = client.get_endpoint(hostname=hostname,
                                       orchestrator_id=orchestrator_id,
                                       workload_id=container_id)
    except KeyError:
        logger.info("ERROR: Container %s doesn't contain any endpoints" % container_id)
        sys.exit(1)

    # Remove any IP address assignments that this endpoint has
    for net in endpoint.ipv4_nets | endpoint.ipv6_nets:
        assert(net.size == 1)
        client.unassign_address(None, net.ip)

    # Remove the endpoint
    netns.remove_veth(endpoint.name)

    # Remove the container from the datastore.
    client.remove_workload(hostname=hostname, 
                           orchestrator_id=orchestrator_id, 
                           workload_id=container_id)

    logger.info("Removed Calico interface from %s" % container_id)

def _create_profile(endpoint, profile_name, ip, client):
    """
    Configure the calico profile to the endpoint
    """
    logger.info('Configuring Pod Profile: %s' % profile_name)

    if client.profile_exists(profile_name):
        logger.info("Profile with name %s already exists, applying to endpoint." % (profile_name))

    else:
        logger.info("Creating profile %s." % (profile_name))
        client.create_profile(profile_name)
        # _apply_rules(profile_name, client)

    # Also set the profile for the workload.
    client.set_profiles_on_endpoint(profile_names=[profile_name], 
                                    endpoint_id=endpoint.endpoint_id)

    dump = json.dumps(
        {
            "ip4": {
                "ip": "%s/24" % ip
            }
        })
    print(dump)

def _create_rules(profile):
    """
    Create a json dict of rules for calico profiles
    """
    rules_dict = {
        "id": profile,
        "inbound_rules": [
            {
                "action": "allow",
            },
        ],
        "outbound_rules": [
            {
                "action": "allow",
            },
        ],
    }
    rules_json = json.dumps(rules_dict, indent=2)
    rules = Rules.from_json(rules_json)
    return rules

def _apply_rules(profile_name, client):
    """
    Generate a new profile rule list and update the client
    :param profile_name: The profile to update
    :type profile_name: string
    :return:
    """
    try:
        profile = client.get_profile(profile_name)
    except:
        logger.info("ERROR: Could not apply rules. Profile not found: %s, exiting" % profile_name)
        sys.exit(1)

    profile.rules = _create_rules(profile_name)
    client.profile_update_rules(profile)
    logger.info("Finished applying rules.")


def _generate_pool(client):
    """
    Take Input subnet (global), create IP pool in datastore
    Will complete silently if it exists
    return IPPool  object of subnet pool
    """
    try:
        subnet = INPUT_JSON['ipam']['subnet']
    except KeyError:
        logger.info("ERROR: Pool not specified in config")
        sys.exit(1)

    pool = IPPool(subnet)
    version = IPNetwork(subnet).version

    client.add_ip_pool(version, pool)
    logger.info("Using Pool %s" % pool)

    return pool

def _allocate_IP(pool):
    """
    Determine next available IP for given pool and assign it
    :param IPPool or IPNetwork pool: The pool to get assignments for.
    :return: The next avail address from the pool
    :rtype IPAddress object
    """
    candidate = SequentialAssignment().allocate(pool)
    logger.info("Using IP %s" % candidate)
    return IPAddress(candidate)

if __name__ == '__main__':
    env = os.environ.copy()
    env[ETCD_AUTHORITY_ENV] = 'localhost:2379'  if ETCD_AUTHORITY_ENV not in env.keys() else env[ETCD_AUTHORITY_ENV]

    input_ = ''.join(sys.stdin.readlines()).replace('\n', '')
    INPUT_JSON = json.loads(input_).copy()

    configure_logger(logger)
        
    main()
