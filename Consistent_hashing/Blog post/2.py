from hashlib import md5
from struct import unpack_from

def func(new_node_count):
    NODE_COUNT = 100
    NEW_NODE_COUNT = new_node_count 
    DATA_ID_COUNT = 10000000

    moved_ids = 0

    for data_id in range(DATA_ID_COUNT):

        data_id = str(data_id)
        hsh = unpack_from('>I', md5(str(data_id)).digest())[0]
        node_id = hsh % NODE_COUNT
        new_node_id = hsh % NEW_NODE_COUNT

        if node_id != new_node_id:
            moved_ids += 1

    percent_moved = 100.0 * moved_ids / DATA_ID_COUNT
    print '%d , %d , %.02f%%' % (NEW_NODE_COUNT,moved_ids, percent_moved)

for i in range(100,120,2):
    func(i)