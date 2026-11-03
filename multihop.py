#!/usr/bin/env python3
"""
Run on a Linux VM with Mininet installed:
    sudo -E python3 multihop.py --bw-mbps 1 --delay-ms 20 --loss-pct 0

This emulates Linux network interfaces/queues, not Python application delays.
"""
import argparse

from mininet.cli import CLI
from mininet.link import TCLink
from mininet.log import info, setLogLevel
from mininet.net import Mininet


def configure(net):
    c1, c2, r1, r2, srv = (net[n] for n in ('c1', 'c2', 'r1', 'r2', 'srv'))

    # Every interface is explicitly numbered; no OpenFlow switches/controllers.
    for node, intf, addr in [
        (c1, 'c1-eth0', '10.10.1.2/24'),
        (c2, 'c2-eth0', '10.10.2.2/24'),
        (r1, 'r1-eth0', '10.10.1.1/24'),
        (r1, 'r1-eth1', '10.10.2.1/24'),
        (r1, 'r1-eth2', '10.10.12.1/30'),
        (r2, 'r2-eth0', '10.10.12.2/30'),
        (r2, 'r2-eth1', '10.10.3.1/24'),
        (srv, 'srv-eth0', '10.10.3.2/24'),
    ]:
        node.setIP(addr, intf=intf)

    for router in (r1, r2):
        router.cmd('sysctl -w net.ipv4.ip_forward=1')

    for node, gw in [(c1, '10.10.1.1'), (c2, '10.10.2.1'), (srv, '10.10.3.1')]:
        node.cmd(f'ip route replace default via {gw}')

    r1.cmd('ip route replace 10.10.3.0/24 via 10.10.12.2')
    r2.cmd('ip route replace 10.10.1.0/24 via 10.10.12.1')
    r2.cmd('ip route replace 10.10.2.0/24 via 10.10.12.1')


def main():
    ap = argparse.ArgumentParser(description='Coin Rush two-router Mininet network')
    ap.add_argument('--bw-mbps', type=float, default=1.0,
                    help='r1-r2 bottleneck bandwidth in Mbps (0.15 = 150 kbps)')
    ap.add_argument('--delay-ms', type=float, default=20,
                    help='one-way netem delay on EACH direction of r1-r2 link')
    ap.add_argument('--loss-pct', type=float, default=0,
                    help='netem loss percentage on each direction of r1-r2 link')
    ap.add_argument('--queue-packets', type=int, default=100,
                    help='netem queue limit, packets (not bytes)')
    args = ap.parse_args()
    if args.bw_mbps <= 0 or args.delay_ms < 0 or not 0 <= args.loss_pct <= 100 or args.queue_packets < 1:
        ap.error('Require positive bandwidth/queue, nonnegative delay, and 0-100% loss')

    net = Mininet(controller=None, link=TCLink, build=False, autoSetMacs=True)
    try:
        c1 = net.addHost('c1')
        c2 = net.addHost('c2')
        r1 = net.addHost('r1')
        r2 = net.addHost('r2')
        srv = net.addHost('srv')
        edge = {'bw': 100, 'delay': '1ms'}
        middle = {'bw': args.bw_mbps, 'delay': f'{args.delay_ms}ms',
                  'loss': args.loss_pct, 'max_queue_size': args.queue_packets}

        net.addLink(c1, r1, intfName1='c1-eth0', intfName2='r1-eth0', **edge)
        net.addLink(c2, r1, intfName1='c2-eth0', intfName2='r1-eth1', **edge)
        net.addLink(r1, r2, intfName1='r1-eth2', intfName2='r2-eth0', **middle)
        net.addLink(r2, srv, intfName1='r2-eth1', intfName2='srv-eth0', **edge)

        net.build()
        net.start()
        configure(net)

        info('\n*** Topology: c1/c2 -> r1 -> r2 -> srv (two routed devices)\n')
        info(f'*** Bottleneck r1-r2: {args.bw_mbps:g} Mbps, '
             f'{args.delay_ms:g} ms one-way per direction, '
             f'{args.loss_pct:g}% link loss, queue {args.queue_packets} packets\n')
        info('*** Server address: 10.10.3.2\n')
        info('*** Test: c1 ping -c 3 10.10.3.2\n')
        info('*** Inspect route: c1 traceroute -n 10.10.3.2\n')
        info('*** GUI (Linux desktop required): xterm srv c1 c2\n\n')
        CLI(net)
    finally:
        net.stop()


if __name__ == '__main__':
    setLogLevel('info')
    main()
