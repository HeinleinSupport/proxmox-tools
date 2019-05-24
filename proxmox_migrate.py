#!/usr/bin/env python
# -*- encoding: utf-8; py-indent-offset: 4 -*-

# (c) 2019 Heinlein Support GmbH
#          Robert Sander <r.sander@heinlein-support.de>

# This is free software;  you can redistribute it and/or modify it
# under the  terms of the  GNU General Public License  as published by
# the Free Software Foundation in version 3.  This file is distributed
# in the hope that it will be useful, but WITHOUT ANY WARRANTY;  with-
# out even the implied warranty of  MERCHANTABILITY  or  FITNESS FOR A
# PARTICULAR PURPOSE. See the  GNU General Public License for more de-
# ails.  You should have  received  a copy of the  GNU  General Public
# License along with GNU Make; see the file  COPYING.  If  not,  write
# to the Free Software Foundation, Inc., 51 Franklin St,  Fifth Floor,
# Boston, MA 02110-1301 USA.


from pprint import pprint

import argparse
import time
import sys
from proxmoxer import ProxmoxAPI

class ProxmoxAPIext(ProxmoxAPI):
    def migrate_vm(self, vm, dest):
        now = int(time.time())
        source = vm['node']
        if source != dest:
            print "Migrating VM %s (%s) from %s to %s" % (vm['vmid'], vm['name'], source, dest)
            taskid = self.nodes(source).post('%s/migrate' % vm['id'], target=dest, online=1)
            if args.wait:
                if ':hamigrate:' in taskid:
                    taskid = False
                    print "Waiting for HA migration task to start",
                    sys.stdout.flush()
                    while not taskid:
                        time.sleep(1)
                        print '.',
                        sys.stdout.flush()
                        for task in filter(lambda x: x['id'] == str(vm['vmid']) \
                                               and ':qmigrate:' in x['upid'] \
                                               and x['starttime'] > now,
                                           self.cluster.tasks.get()):
                            taskid = task['upid']
                            print
                print taskid
                finished = False
                print "Waiting for task to finish",
                sys.stdout.flush()
                while not finished:
                    time.sleep(1)
                    print ".",
                    sys.stdout.flush()
                    status = self.nodes(source).tasks(taskid).status.get()
                    if status['status'] != 'running':
                        finished = status
                print " finished"
                if finished['exitstatus'] != 'OK':
                    return False
            else:
                print "started %s" % taskid
        return True

    def migrate_vmid(self, vmid, dest):
        vm = filter(lambda x: x['vmid'] == vmid, self.cluster.resources.get(type='vm'))[0]
        return self.migrate_vm(vm, dest)

    def get_groups(self):
        groups = {}
        for group in self.cluster.ha.groups.get():
            groups[group['group']] = group
            groups[group['group']][u'nodelist'] = map(lambda x: x.split(':')[0], group['nodes'].split(','))
        return groups

    def get_ha_resources(self):
        groups = self.get_groups()
        resources = {}
        for res in self.cluster.ha.resources.get():
            id = int(res['sid'].split(':')[1])
            resources[id] = res
            if 'group' in res and res['group'] in groups:
                resources[id][u'group'] = groups[res['group']]
            else:
                resources[id][u'group'] = {u'nodelist': []}
        return resources

    def get_vms(self, filterfunc = lambda x: x):
        vms = {}
        for vm in filter(filterfunc, proxmox.cluster.resources.get(type='vm')):
            vms[vm['vmid']] = vm
        return vms

    def get_nodes(self, nodelist, maxfree = False):
        nodes = []
        for node in self.nodes.get():
            if node['node'] in nodelist:
                if maxfree:
                    node[u'memfree'] = node['maxmem']
                else:
                    node[u'memfree'] = node['maxmem'] - node['mem']
                nodes.append(node)
        return nodes

    def get_dstnodes_bymem(self, nodelist, totalneeded, maxfree = False):
        dstnodes = self.get_nodes(nodelist, maxfree)
        totalfree = reduce(lambda x,y: x+y, map(lambda x: x['memfree'], dstnodes))
        if args.debug:
            print "totalfree:", totalfree
        if totalfree < totalneeded:
            print "Unable to evacuate, not enough RAM free."
            sys.exit(1)
        for dstnode in dstnodes:
            dstnode[u'memperc'] = float(dstnode['memfree']) / float(totalfree)
        dstnodes_bymem = sorted(dstnodes, key=lambda x: x['memperc'], reverse=True)
        return dstnodes_bymem

    def balance_vms(self, vms, dstnodes, maxfree = False):
        res = self.get_ha_resources()
        if args.debug:
            pprint(res)

        migrate = {}
        for dstnode in proxmox.get_nodes(dstnodes, True):
            migrate[dstnode['node']] = []
        lenvms = len(vms)
        totalneeded = reduce(lambda x,y: x+y, map(lambda x: x['mem'], vms.values()), 0)
        if args.debug:
            print "needed:", totalneeded
        dstnodes_bymem = proxmox.get_dstnodes_bymem(dstnodes, totalneeded, maxfree)
        firstrun = True
        while len(vms):
            dstnodes_bymem = sorted(dstnodes_bymem, key=lambda x: x['memfree'], reverse=True)
            if args.debug:
                print "dstnodes_bymem: %s" % map(lambda x: x['node'], dstnodes_bymem)
            vms_bymem = map(lambda x: x['vmid'], sorted(vms.values(), key=lambda x: x['mem'], reverse=True))
            if args.debug:
                print "vms_bymem: %s" % vms_bymem
            firstbatch = 0
            if args.debug:
                print "firstbatch = 0"
            dstnodes_seen = {}
            if firstrun:
                vms_sit = len(dstnodes_bymem)
                for vmid in vms_bymem:
                    if not vms_sit:
                        break
                    if vms[vmid]['node'] not in dstnodes_seen and vms[vmid]['node'] in map(lambda x: x['node'], dstnodes_bymem):
                        print "%s (%s) stays on %s" % (vmid, vms[vmid]['name'], vms[vmid]['node'])
                        dstnodes_seen[vms[vmid]['node']] = vms[vmid]['mem']
                        vms_sit -= 1
                        del(vms[vmid])
                for dstnode in dstnodes_bymem:
                    if dstnode['node'] in dstnodes_seen:
                        dstnode['memfree'] -= dstnodes_seen[dstnode['node']]
                dstnodes_bymem = sorted(dstnodes_bymem, key=lambda x: x['memfree'], reverse=True)
                firstrun = False
            if args.debug:
                print "dstnodes_bymem: %s" % map(lambda x: x['node'], dstnodes_bymem)
            for dstnode in dstnodes_bymem:
                if dstnode['node'] in dstnodes_seen:
                    dstnode['memfree'] -= dstnodes_seen[dstnode['node']]
                    firstbatch = dstnodes_seen[dstnode['node']]
                    break
                if args.debug:
                    print dstnode['node'], dstnode['memfree'], "%02.f%%" % (dstnode['memfree'] * 100.0 / dstnode['maxmem'])
                vms_bymem = map(lambda x: x['vmid'], sorted(vms.values(), key=lambda x: x['mem'], reverse=True))
                if args.debug:
                    pprint(vms_bymem)
                batchtotal = 0
                for vmid in vms_bymem:
                    batchtotal += vms[vmid]['mem']
                    if args.debug:
                        print vmid, dstnode['memfree'], "%02.f%%" % (dstnode['memfree'] * 100.0 / dstnode['maxmem']), vms[vmid]['mem'], firstbatch, batchtotal
                    # is enough mem free on current node?
                    memfree = dstnode['memfree'] > vms[vmid]['mem']
                    # is current node first in node list?
                    first = firstbatch == 0
                    # is current batch smaller than first batch?
                    second = batchtotal <= firstbatch
                    # is vm HA resource?
                    haresource = vmid in res
                    # is VM HA managed (started)?
                    started = haresource and res[vmid]['state'] == 'started'
                    # is VM for current node based on HA group?
                    forcurrentnode = haresource and (dstnode['node'] in res[vmid]['group']['nodelist'] or not res[vmid]['group']['nodelist'] or not res[vmid]['group']['restricted'])
                    if args.debug:
                        print memfree, first, second, haresource, started, forcurrentnode
                    if memfree and \
                       (first or second) and \
                       (started and forcurrentnode or not started or not haresource):
                        dstnode['memfree'] -= vms[vmid]['mem']
                        if args.debug:
                            print "  ", dstnode['node'], dstnode['memfree'], "%02.f%%" % (dstnode['memfree'] * 100.0 / dstnode['maxmem']), vmid, vms[vmid]['mem']
                        migrate[dstnode['node']].append(vms[vmid])
                        if firstbatch == 0:
                            firstbatch = vms[vmid]['mem']
                            del(vms[vmid])
                            break
                        del(vms[vmid])
                    else:
                        batchtotal -= vms[vmid]['mem']
            if lenvms == len(vms):
                for vm in vms.values():
                    print "Unable to find destination for %s (%s)" % (vm['vmid'], vm['name'])
                break
            lenvms = len(vms)
        if args.debug:
            pprint(migrate)
        for dstnode, vms in migrate.iteritems():
            for vm in vms:
                if vm['node'] == dstnode:
                    print "%s (%s) stays on %s" % (vm['vmid'], vm['name'], vm['node'])
                else:
                    if args.dryrun:
                        print "would migrate %s (%s) from %s to %s" % (vm['vmid'], vm['name'], vm['node'], dstnode)
                    else:
                        proxmox.migrate_vm(vm, dstnode)
        for dstnode in dstnodes_bymem:
            print "%s has %d memory free (%0.2f%%)" % (dstnode['node'], dstnode['memfree'], dstnode['memfree'] * 100.0 / dstnode['maxmem'])

parser = argparse.ArgumentParser()
parser.add_argument('-u', '--username', required=True)
parser.add_argument('-p', '--password', required=True)
parser.add_argument('-n', '--dryrun', action='store_true', required=False)
parser.add_argument('-d', '--debug', action='store_true', required=False)
parser.add_argument('-w', '--wait', action='store_true', required=False)
subparsers = parser.add_subparsers(title='available commands', help='call "subcommand --help" for more information')
evacuate = subparsers.add_parser('evacuate', help='evacuate first host, migrate VMs to other hosts')
evacuate.set_defaults(func='evacuate')
evacuate.add_argument('source', help='source node')
evacuate.add_argument('dstnodes', nargs='+', metavar='node', help='list of destination node names')
balanceram = subparsers.add_parser('balanceram', help='balance VMs over all hosts based on RAM usage')
balanceram.set_defaults(func='balanceram')
balanceram.add_argument('nodes', nargs='+', metavar='node', help='list of node names')
migrate = subparsers.add_parser('migrate', help='migrate one VM to dest')
migrate.set_defaults(func='migrate')
migrate.add_argument('vmid', type=int, help='VM ID')
migrate.add_argument('dst', help='destination node')

args = parser.parse_args()

if args.func == 'evacuate':

    proxmox = ProxmoxAPIext(args.source, user=args.username, password=args.password, verify_ssl=False)

    vms = proxmox.get_vms(lambda x: x['status'] == 'running' and x['node'] == args.source)
    if args.debug:
        pprint(vms)

    proxmox.balance_vms(vms, args.dstnodes)

elif args.func == 'balanceram':
    if len(args.nodes) < 2:
        raise RuntimeError('List of nodes is too short: %s' % ', '.join(args.nodes))

    proxmox = ProxmoxAPIext(args.nodes[0], user=args.username, password=args.password, verify_ssl=False)

    vms = proxmox.get_vms(lambda x: x['status'] == 'running' and x['node'] in args.nodes)
    if args.debug:
        pprint(vms)

    proxmox.balance_vms(vms, args.nodes, True)

elif args.func == 'migrate':

    proxmox = ProxmoxAPIext(args.dst, user=args.username, password=args.password, verify_ssl=False)
    if not proxmox.migrate_vmid(args.vmid, args.dst):
        sys.exit(1)
