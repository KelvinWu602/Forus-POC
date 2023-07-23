import random
import threading
import time
import base64
from Crypto.PublicKey import RSA
from Crypto import Random
from Crypto.Cipher import PKCS1_v1_5 as PKCS1_cipher


class ReliableNetwork():
    def __init__(self, N = 5):
        self.network_buffer = {}    
        self.proxy_buffer = {}
        for sender in range(N):
            for receiver in range(N):
                self.network_buffer[(sender,receiver)] = []
        for proxy in range(N):
            self.proxy_buffer[proxy] = []
        self.proxy_announcement_buffer = []
        self.logs = []


    def send(self, sender, receiver, data):
        self.network_buffer[(sender,receiver)].append(data)
        self.logs.append((sender,receiver,data))

    def receive(self, sender, receiver):
        if len(self.network_buffer[(sender,receiver)])>0:
            return self.network_buffer[(sender,receiver)].pop(0)
        return None
    
    def reportProxy(self, proxy_node_id, job_id, node_id):  
        self.proxy_buffer[proxy_node_id].append((job_id, node_id))
        print('\n     ⭐ Node {}->Proxy : job {}\n'.format(node_id, job_id))
        self.logs.append((node_id, proxy_node_id, 'reportProxy for job {}'.format(job_id)))

    def getProxyReport(self, jobID, proxy_node_id):
        if len(self.proxy_buffer[proxy_node_id])>0:
            if self.proxy_buffer[proxy_node_id][0][0] == jobID:
                return self.proxy_buffer[proxy_node_id].pop(0)
        return None

    def announceProxy(self, jobID, proxy_node_id, payload, reported_nodes):
        self.proxy_announcement_buffer.append((jobID, proxy_node_id, payload, reported_nodes))
        self.logs.append((jobID, proxy_node_id, 'announcement', payload, reported_nodes))
    
    def getProxyAnnouncement(self, jobID):
        if len(self.proxy_announcement_buffer)>0:
            for i in range(len(self.proxy_announcement_buffer)):
                if self.proxy_announcement_buffer[i][0] == jobID:
                    return self.proxy_announcement_buffer[i]
        return None
    

    def printLogs(self):
        with open('logs.txt','w') as f:
            for log in self.logs:
                f.write(str(log).replace('\n','\\n')+'\n')
    
    
class Node():
    def __init__(self, N, node_id, network, health_set, publicKeys, proxy_timeout):
        self.N = N
        self.node_id = node_id
        self.network = network
        self.health_set = health_set
        self.proxy_timeout = proxy_timeout
        self.alive = True
        self.message_sent_buffer = [] # messages to be sent will be first put into this buffer, then sent out by periodic job worker
        for _ in range(N):
            self.message_sent_buffer.append([])
        # load its own private key
        with open('private_key_{}.pem'.format(node_id)) as f:
            data = f.read()
            self.privateKey = RSA.importKey(data)
        self.publicKeys = publicKeys
        self.received_from = {}
        self.sent_to = {}
        self.first_receives = {}
        self.proxy_alarm = {}


    def run(self, timeout, sending_interval):
        # main thread = listening to network
        print('Node {} starts running...'.format(self.node_id))

        # simulation thread for termination
        threading.Thread(target=self.stop, args=(timeout,)).start()
        
        # separate threads for message sending
        for receiver in range(self.N):
            if receiver == self.node_id:
                continue
            threading.Thread(target=self.send_message_worker, args=(receiver, sending_interval)).start()

        # the listening thread
        while self.alive:
            for sender in range(self.N):
                msg = self.network.receive(sender,self.node_id)
                if msg != None:
                    # separate thread to handle messages
                    t = threading.Thread(target=self.handle, args=(sender,msg))
                    t.start()
            time.sleep(1)
    
    def send_message_worker(self, receiver, sending_interval):
        while self.alive:
            time.sleep(sending_interval)
            if len(self.message_sent_buffer[receiver]) > 0:
                # sends a T2 message
                self.network.send(self.node_id, receiver, self.message_sent_buffer[receiver].pop(0))
            elif receiver in self.health_set:
                # sends a T1 message
                receiver_public_key = RSA.importKey(self.publicKeys[receiver])
                cipher = PKCS1_cipher.new(receiver_public_key)
                T1 = base64.b64encode(cipher.encrypt(bytes(("f"*128).encode('utf-8'))))
                self.network.send(self.node_id, receiver, T1.decode('utf-8'))

    def stop(self, timeout):
        print("⏱️ simulation timer for node {} = {}s".format(self.node_id,timeout))
        # this node will keep running for timeout seconds
        time.sleep(timeout)
        self.alive = False        

    def handle(self, sender, msg):
        cipher = PKCS1_cipher.new(self.privateKey)
        decrypted = cipher.decrypt(base64.b64decode(msg),0)
        
        if decrypted[-1]%2 == 0:
            # msg is T1, do nothing
            return
        else:
            # msg is T2
            # byte[0- 3] : proxy node ID
            # byte[4- 7] : job ID
            # byte[8-127] : payload
            proxy_node_id = int.from_bytes(decrypted[0:4], 'big')
            job_id = int.from_bytes(decrypted[4:8], 'big')
            payload = decrypted[8:]
    
            print('Node {}->{}, Type: {}, data: (jobID:{}, proxy:{}, payload:{})'.format(sender, self.node_id,'T1' if decrypted[-1]%2 == 0 else 'T2', job_id, proxy_node_id, payload))
            
            if proxy_node_id == self.node_id and job_id not in self.first_receives:
                # I am the proxy node
                threading.Thread(target=self.proxy_job, args=(job_id, payload)).start()
            
            # proxy report
            if job_id in self.received_from:
                # you have received T2 twice
                self.network.reportProxy(proxy_node_id, job_id, self.node_id)

            if job_id not in self.received_from:
                self.received_from[job_id] = {}
                self.first_receives[job_id] = payload
            if job_id not in self.sent_to:
                self.sent_to[job_id] = {}
            
            
            self.received_from[job_id][sender] = self.received_from[job_id].get(sender,0) + 1
            self.forward(job_id,proxy_node_id, payload)

    def publish(self, job_id, proxy_node_id, payload):
        if job_id not in self.received_from:
            self.received_from[job_id] = {}
        if job_id not in self.sent_to:
            self.sent_to[job_id] = {}
        self.forward(job_id, proxy_node_id, payload)

    def forward(self, job_id, proxy_node_id, payload):
        # get candidate set
        received_from = self.received_from[job_id]
        sent_to = self.sent_to[job_id]
        candidates = set([node for node in range(N) if received_from.get(node,0) + sent_to.get(node,0) < 2])
        if self.node_id in candidates:
            candidates.remove(self.node_id)

        # select a node to forward
        forwardTo = None
        if random.random() < 0.5 and len(candidates.intersection(set([proxy_node_id])))>0:
            # 50% to proxy_node, if it is in the candidate set
            forwardTo = proxy_node_id
        elif random.random() < 0.8 and len(candidates.intersection(self.health_set))>0:
            # remaining 80% to health set, if there is any in candidate
            forwardTo = random.choice(list(candidates.intersection(self.health_set)))
        elif len(candidates) > 0:
            # remaining 20% to any node
            forwardTo = random.choice(list(candidates))
        
        # send to forwardTo
        if forwardTo != None:
            receiver_public_key = RSA.importKey(self.publicKeys[forwardTo])
            cipher = PKCS1_cipher.new(receiver_public_key)
            T2 = base64.b64encode(cipher.encrypt(proxy_node_id.to_bytes(4,'big') + job_id.to_bytes(4,'big') + payload))
            # last moment check if the job is still alive
            if self.network.getProxyAnnouncement(job_id) == None:
                self.message_sent_buffer[forwardTo].append(T2.decode('utf-8'))
                self.sent_to[job_id][forwardTo] = self.sent_to[job_id].get(forwardTo,0) + 1
            else:
                print('Node {} copy job {} terminated'.format(self.node_id, job_id))
        

    def proxy_job(self, jobID, payload):
        self.proxy_alarm[jobID] = True
        # stop after self.proxy_timeout seconds
        threading.Thread(target=self.proxy_job_timeout, args=(jobID,)).start()

        reported_node = set({})
        while self.proxy_alarm[jobID]:
            # print('proxy reported nodes: {}'.format(reported_node))
            # check if there is any report from other nodes
            report = self.network.getProxyReport(jobID,self.node_id)
            if report != None:
                reported_node.add(report[1])
            
            # check if all nodes have reported
            if len(reported_node) == self.N:
                print("\n⭕ Job {} completed. payload = {}\n".format(jobID, payload))
                self.network.announceProxy(jobID, self.node_id, payload, reported_node)
                return
            time.sleep(0.1)
        print("\n❌ Job {} timeout. payload = {}. Reported: {}".format(jobID, payload, reported_node))
        

    def proxy_job_timeout(self, jobID):
        print("start proxy job timeout: {}s".format(self.proxy_timeout))
        time.sleep(self.proxy_timeout)
        self.proxy_alarm[jobID] = False
        # print("proxy job timeout expired: {}s".format(self.proxy_timeout))

N = 5
H = 2
simulation_length = 30
sending_interval = 1
publisher = 0
proxy_timeout = 20
jobID = 138
proxy_node_id = 2
nBranches = 2
content = "hello worlde"
assert H <= N      
assert proxy_timeout < simulation_length
assert proxy_node_id < N
assert nBranches <= H
assert publisher < N
assert bytes(content.encode('utf-8'))[-1] %2 == 1  # last byte of content must be odd, ie T2
reliableNetwork = ReliableNetwork(N)

# create public and private keys
public_keys = []
for i in range(N):
    random_generator = Random.new().read
    rsa = RSA.generate(2048, random_generator)
    private_key = rsa.exportKey()
    public_key = rsa.publickey().exportKey()
    public_keys.append(public_key)
    with open('public_key_{}.pem'.format(i),'wb') as f:
        f.write(public_key)
    with open('private_key_{}.pem'.format(i),'wb') as f:
        f.write(private_key)

nodes = [Node(N,i,reliableNetwork,set([(i+j+1)%N for j in range(H)]), public_keys, proxy_timeout) for i in range(N)]

ths = []
# start simulation
for i in range(N):
    ths.append(threading.Thread(target=nodes[i].run, args=(simulation_length, sending_interval)))
    ths[i].start()

for _ in range(nBranches):
    nodes[0].publish(jobID,proxy_node_id,bytes(content.encode('utf-8')))
    
for t in ths:
    t.join()

print("\n 🆗Simulation ended. Check logs.txt for full network traffics.")
reliableNetwork.printLogs()