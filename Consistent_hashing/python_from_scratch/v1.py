
import ConsistentHashRing

if __name__ == '__main__':
    server_infos = ["redis1", "redis2", "redis3", "redis4"];
    hash_ring = ConsistentHashRing()
    test_keys = ["-84942321036308",
        "-76029520310209",
        "-68343931116147",
        "-54921760962352",
        "-53401599829545"
        ];

    for server in server_infos:
        hash_ring[server] = server

    for key in test_keys:
        print str(hash_ring[key])