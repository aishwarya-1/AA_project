import bisect
import hashlib

class ConsistentHash:

  def __init__(self,num_machines=1,num_replicas=1):

    self.num_machines = num_machines
    self.num_replicas = num_replicas
    
    hash_tuples =  [
                    (j,k,my_hash(str(j)+"_"+str(k))) \
                   for j in range(self.num_machines) \
                   for k in range(self.num_replicas)
                   ]

    # Sort the tuples based on hash values only
    hash_tuples.sort(lambda x,y: cmp(x[2],y[2]))
    self.hash_tuples = hash_tuples

  def get_machine(self,key):

    #Returns the number of the machine which key gets sent to.
    h = my_hash(key)
    # edge case where we cycle past hash value of 1 and back to 0.
    if h > self.hash_tuples[-1][2]: return self.hash_tuples[0][0]
    hash_values = map(lambda x: x[2],self.hash_tuples)
    index = bisect.bisect_left(hash_values,h)
    return self.hash_tuples[index][0]


  def remove_machine(self):

    self.num_machines = self.num_machines - 1
    self.num_replicas = self.num_replicas

    hash_tuples = [
                    (j,k,my_hash(str(j)+"_"+str(k))) \
                for j in range(self.num_machines) \
                for k in range(self.num_replicas)
                    ]

    hash_tuples.sort(lambda x,y: cmp(x[2],y[2]))
    self.hash_tuples = hash_tuples
            
  def add_machine(self):

    self.num_machines = self.num_machines + 1
    self.num_replicas = self.num_replicas

    hash_tuples = [
                    (j,k,my_hash(str(j)+"_"+str(k))) \
                for j in range(self.num_machines) \
                for k in range(self.num_replicas)
                    ]

    hash_tuples.sort(lambda x,y: cmp(x[2],y[2]))
    self.hash_tuples = hash_tuples

def my_hash(key):

  '''my_hash(key) returns a hash in the range [0,1).'''
  return (int(hashlib.md5(key).hexdigest(),16) % 1000000)/1000000.0

def main():

    print("Initializing ring with 1 machine with 3 replicas")
    ch = ConsistentHash(1,3)

    print "Format:"
    print "(machine,replica,hash value):"
    for (j,k,h) in ch.hash_tuples: print "(%s,%s,%s)" % (j,k,h)

    print("Adding 5 more machines")
    ch.add_machine()
    ch.add_machine()
    ch.add_machine()
    ch.add_machine()
    ch.add_machine()

    print "Format:"
    print "(machine,replica,hash value):"
    for (j,k,h) in ch.hash_tuples: 
        print "(%s,%s,%s)" % (j,k,h)

    print("Removing a machine")
    ch.remove_machine()

    print "Format:"
    print "(machine,replica,hash value):"
    for (j,k,h) in ch.hash_tuples:
        print "(%s,%s,%s)" % (j,k,h)

    ch.add_machine()

    print "Format:"
    print "(machine,replica,hash value):"
    for (j,k,h) in ch.hash_tuples: 
        print "(%s,%s,%s)" % (j,k,h)

    while True:
        print "\nPlease enter a key:"
        key = raw_input()
        print "\nKey %s maps to hash %s, and so to machine %s" \
            % (key,my_hash(key),ch.get_machine(key))

if __name__ == "__main__": 
    main()