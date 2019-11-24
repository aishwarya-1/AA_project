from array import array
from hashlib import md5
from random import shuffle
from struct import unpack_from
from time import time

class Ring(object):

    def __init__(self, nodes, part2node, replicas):
        self.nodes = nodes
        self.part2node = part2node
        self.replicas = replicas
        partition_power = 1
        while 2 ** partition_power < len(part2node):
            partition_power += 1
        if len(part2node) != 2 ** partition_power:
            raise Exception("part2node's length is not an "
                            "exact power of 2")
        self.partition_shift = 32 - partition_power

    def get_nodes(self, data_id):
        data_id = str(data_id)
        part = unpack_from('>I',
           md5(data_id).digest())[0] >> self.partition_shift
        node_ids = [self.part2node[part]]
        zones = [self.nodes[node_ids[0]]]
        for replica in range(1, self.replicas):
            while self.part2node[part] in node_ids and \
                   self.nodes[self.part2node[part]] in zones:
                part += 1
                if part >= len(self.part2node):
                    part = 0
            node_ids.append(self.part2node[part])
            zones.append(self.nodes[node_ids[-1]])
        return [self.nodes[n] for n in node_ids]

def build_ring(nodes, partition_power, replicas):
    begin = time()
    part2node = array('H')
    for part in range(2 ** partition_power):
        part2node.append(part % len(nodes))
    shuffle(part2node)
    ring = Ring(nodes, part2node, replicas)
    print '%.02fs to build ring' % (time() - begin)
    return ring

def test_ring(ring):
    begin = time()
    DATA_ID_COUNT = 10000000
    node_counts = {}
    zone_counts = {}
    for data_id in range(DATA_ID_COUNT):
        for node in ring.get_nodes(data_id):
            node_counts[node['id']] = \
                node_counts.get(node['id'], 0) + 1
            zone_counts[node['zone']] = \
                zone_counts.get(node['zone'], 0) + 1
    print '%ds to test ring' % (time() - begin)
    desired_count = \
        DATA_ID_COUNT / len(ring.nodes) * REPLICAS
    print '%d: Desired data ids per node' % desired_count
    max_count = max(node_counts.values())
    over = \
        100.0 * (max_count - desired_count) / desired_count
    print '%d: Most data ids on one node, %.02f%% over' % \
        (max_count, over)
    min_count = min(node_counts.values())
    under = \
        100.0 * (desired_count - min_count) / desired_count
    print '%d: Least data ids on one node, %.02f%% under' % \
        (min_count, under)
    zone_count = \
        len(set(n['zone'] for n in ring.nodes.values()))
    desired_count = \
        DATA_ID_COUNT / zone_count * ring.replicas
    print '%d: Desired data ids per zone' % desired_count
    max_count = max(zone_counts.values())
    over = \
        100.0 * (max_count - desired_count) / desired_count
    print '%d: Most data ids in one zone, %.02f%% over' % \
        (max_count, over)
    min_count = min(zone_counts.values())
    under = \
        100.0 * (desired_count - min_count) / desired_count
    print '%d: Least data ids in one zone, %.02f%% under' % \
        (min_count, under)

if __name__ == '__main__':
    PARTITION_POWER = 16
    REPLICAS = 3
    NODE_COUNT = 256
    ZONE_COUNT = 16
    nodes = {}
    while len(nodes) < NODE_COUNT:
        zone = 0
        while zone < ZONE_COUNT and len(nodes) < NODE_COUNT:
            node_id = len(nodes)
            nodes[node_id] = {'id': node_id, 'zone': zone}
            zone += 1
    ring = build_ring(nodes, PARTITION_POWER, REPLICAS)
    test_ring(ring)