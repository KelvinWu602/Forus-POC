import random
import threading
import time

class SecretRounding():
    def __init__(self, N = 5, health_sets = None):
        self.N = N
        if health_sets == None:
            self.health_sets = [0]*self.N
            for i in range(self.N):
                self.health_sets[i] = {(i+1)%self.N , (i+2)%self.N, (i+3)%self.N, (i+4)%self.N}

    
        
    def draw_from_health_set(self, i ,k):
        res = set({})
        while len(res) < k:
            c = random.choice(list(self.health_sets[i]))
            if c not in res:
                res.add(c)
        return res
    
    def init_job(self):
        self.sent_to = [0]*self.N
        self.receive_from = [0]*self.N
        self.reported = set({})
        self.threads = []
        for i in range(self.N):
            self.sent_to[i] = [0]*self.N
            self.receive_from[i] = [0]*self.N

        self.byzantine = [True if random.random()<0.5 else False for _ in range(self.N)]

    def get_candidates(self, i):
        candidates = set({})
        for j in range(self.N):
            if j == i: 
                continue
            if self.receive_from[i][j] == 0:
                if self.sent_to[i][j] < 2:
                    candidates.add(j)
            else:
                if self.sent_to[i][j] < 1:
                    candidates.add(j)
        return candidates
            
    def publish(self, i, nthread = 1,network_delay = 1 , p = 0.8):
        self.init_job()
        hs = self.draw_from_health_set(i, nthread)
        selected = set({})
        for _ in range(nthread):
            if random.random() < p:
                forwardTo = random.choice(list(hs.intersection(self.get_candidates(i)) - selected))
            else:
                forwardTo = random.choice(list(self.get_candidates(i)- selected))
            try:
                t = threading.Thread(target=self.forward, args=(i, forwardTo, 1, nthread, network_delay, p, self.byzantine[forwardTo]))
                selected.add(forwardTo)
                self.threads.append(t)
                t.start()
            except:
                print("Error: unable to start thread {} to {}".format(i, forwardTo))
        print('timeout:' , (network_delay+0.001)*self.N*3)
        time.sleep((network_delay+0.001)*self.N*3)
        print("timeout!!!!", "reported:", self.reported, "byzantines:", [i for i in range(self.N) if self.byzantine[i]])
        

    def check_terminate(self):
        for i in range(self.N):
            if i not in self.reported:
                return False
        return True

    def byzantineP(self, i, step, network_delay = 1):
        # do nothing
        pass
        

    def forward(self, i, j, step ,nthread = 1, network_delay = 1 , p = 0.8, byzantine = False):
        # print("forwarding from {} to {}".format(i, j), "to health set?:" , j in self.health_sets[i]," {}'s candidates: ".format(i), self.get_candidates(i), "{}'s health set:".format(i), self.health_sets[i])
        time.sleep(network_delay)
        print("Time {}: forwarding from {} to {}".format(step, i, j), j, 'is ', 'honest' if not byzantine else 'byzantine')
        self.sent_to[i][j] += 1
        
        if not byzantine:
            self.receive_from[j][i] += 1

            if sum(self.receive_from[j]) >=2 :
                self.reported.add(j)
                print("report received by {}!".format(j), 'reported:', sorted(self.reported))
            
            if self.check_terminate():
                print("terminated!")
                return
                

            calls = 0
            selected = set({})
            for _ in range(nthread):
                forwardTo = None
                if random.random() < p:
                    candidates = list(self.draw_from_health_set(j, nthread).intersection(self.get_candidates(j)) - selected)
                    if len(candidates) > 0:
                        forwardTo = random.choice(candidates)
                    elif len(self.get_candidates(j)- selected) > 0:
                        forwardTo = random.choice(list(self.get_candidates(j)))
                else:
                    candidates = list(self.get_candidates(j)-selected)
                    if len(candidates) > 0:
                        forwardTo = random.choice(candidates)
                if forwardTo != None:
                    try:
                        t = threading.Thread(target=self.forward, args=(j, forwardTo,step+1, nthread, network_delay,p, self.byzantine[forwardTo]))
                        selected.add(forwardTo)
                        self.threads.append(t)
                        t.start()
                    except:
                        print("Error: unable to start thread {} to {}".format(j, forwardTo, step+1))
        else:
            self.byzantineP(j, step+1, network_delay)

    def join(self):
        for t in self.threads:
            t.join()
        
lt = time.time()
a = SecretRounding(20)
for _ in range(1):
    a.publish(0,nthread=3,network_delay=0,p=0.8)
a.join()
et = time.time()
print('time:',(et-lt)*1000,'ms')
print("reported:", a.reported, "byzantines:", [i for i in range(a.N) if a.byzantine[i]])
by = [i for i in range(a.N) if a.byzantine[i]]
print("Reported honest:", len(a.reported)*100.0 / (a.N - len(by)), "%")
print("Non reported honest:", 100.0 - len(a.reported)*100.0 / (a.N - len(by)), "%")
print("Byzantine:", 100.0 - len(a.reported)*100.0 / a.N, "%")

