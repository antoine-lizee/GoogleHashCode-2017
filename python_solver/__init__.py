import os
from pprint import pprint

import datetime
from random import randint, choice


class World:
    def __init__(self, file_path):
        print(file_path)
        self.filename = os.path.splitext(os.path.basename(file_path))[0]
        with open(file_path) as f:
            first_line = next(f).strip()
            self.n_V, self.n_E, self.n_R, self.n_C, self.cs_capacity = (int(v) for v in first_line.strip().split(' '))
            pprint(self.__dict__)
            self.videos = [int(v) for v in next(f).strip().split(' ')]
            self.endpoints = []
            for i in range(self.n_E):
                dc_latency, n_cs = (int(v) for v in next(f).strip().split(' '))
                latencies = {}
                for k in range(n_cs):
                    id, lat = (int(v) for v in next(f).strip().split(' '))
                    latencies[id] = lat
                self.endpoints.append({
                    'dc_lat': dc_latency,
                    'latencies': latencies
                })
            self.tot_requests = 0
            self.requests = []
            for i in range(self.n_R):
                video_id, endpoint_id, n_requests = (int(v) for v in next(f).strip().split(' '))
                self.requests.append([video_id, endpoint_id, n_requests])
                self.tot_requests += n_requests
            self.reset_cache_servers()
            self.vid_to_cs = []

    def reset_cache_servers(self):
        self.cache_servers = []
        for i in range(self.n_C):
            self.cache_servers.append([self.cs_capacity, set()])

    def write_solution(self, prefix=''):
        filename = 'output/' + self.filename + prefix + datetime.datetime.now().isoformat() + '.txt'
        cs = []
        for i, (cap, vids) in enumerate(self.cache_servers):
            if vids:
                cs.append('%d %s' % (i, ' '.join((str(v) for v in vids))))
        with open(filename, 'w') as f:
            f.write(str(len(cs)) + '\n')
            for line in cs:
                f.write(line + '\n')

    def score(self):
        tot_score = 0.0
        tot = 0.0
        for (video_id, endpoint_id, n_requests) in self.requests:
            latencies = self.endpoints[endpoint_id]['latencies']
            lats = [self.endpoints[endpoint_id]['dc_lat']]
            for cs_id, lat in latencies.items():
                if video_id in self.cache_servers[cs_id][1]:
                    lats.append(lat)
            tot_score += (self.endpoints[endpoint_id]['dc_lat'] - min(lats)) * n_requests
            tot += n_requests
        return tot_score / tot * 1000

    def best_lat_gain_for_e(self, endpoint_id, video_id):
        size = self.videos[video_id]
        min_lat = self.endpoints[endpoint_id]['dc_lat']
        best_cs = None
        for cs_id, lat in self.endpoints[endpoint_id]['latencies'].items():
            if self.cache_servers[cs_id][0] > size:
                if min_lat > lat:
                    best_cs = cs_id
                    min_lat = lat
        return best_cs, self.endpoints[endpoint_id]['dc_lat'] - min_lat

    def add_vid_to_cs(self, vid_id, cs_id):
        if cs_id is not None:
            cs = self.cache_servers[cs_id]
            size = self.videos[vid_id]
            if size < cs[0]:
                cs[0] -= size
                cs[1].add(vid_id)

    def algo2(self):
        rs = self.requests
        while rs:
            best_gain = 0
            best_request = None
            best_cs = None
            i_best = None
            for i, r in enumerate(rs):
                cs_id, gain = self.best_lat_gain_for_e(r[1], r[0])
                if best_gain < gain:
                    best_request = r
                    best_cs = cs_id
                    best_gain = gain
                    i_best = i
            if not best_request:
                next
            self.add_vid_to_cs(best_request[0], best_cs)
            rs.pop(i_best)

    def util(self, r):
        return self.best_lat_gain_for_e(r[1], r[0])[1] * r[2] # * (self.videos[r[0]]) ** (2)

    def algo(self, n):
        self.reset_cache_servers()
        w = self.n_R // n
        requests = sorted(
            self.requests,
            key=self.util
        )
        for i in range(n):
            # print("%d/%d, %d/%d" % (i, n-1, w, len(requests)))
            if i == n-1:
                slice = requests
            else:
                slice = requests[:w]
            for r in slice:
                cs, lat = self.best_lat_gain_for_e(r[1], r[0])
                self.add_vid_to_cs(r[0], cs)
            requests = sorted(
                requests[w:],
                key=self.util
            )
        return self.score()

    def score_for_r(self, r):
        latencies = self.endpoints[r[1]]['latencies']
        lats = [self.endpoints[r[1]]['dc_lat']]
        for cs_id, lat in latencies.items():
            if r[0] in self.cache_servers[cs_id][1]:
                lats.append(lat)
        return (self.endpoints[r[1]]['dc_lat'] - min(lats)) * r[2] / self.tot_requests * 1000

    def greedy(self, n, m, verbose=True):
        for i in range(n):
            cs_id = randint(0, self.n_C - 1)
            cs = self.cache_servers[cs_id]
            vid_id = choice(tuple(cs[1]))
            requests = [r for r in self.requests if r[0] == vid_id]
            if verbose:
                print('REMOVING vid %d from %d' % (vid_id, cs_id))
            size = self.videos[vid_id]
            s1 = sum([self.score_for_r(r) for r in requests])
            cs[1].remove(vid_id)
            s2 = sum([self.score_for_r(r) for r in requests])
            score_loss = s1 - s2
            if verbose:
                print('sl: %d' % score_loss)
            success = False
            for j in range(m):
                new_vid_id = randint(0, self.n_V - 1)
                if self.videos[new_vid_id] < size and new_vid_id not in cs[1]:
                    new_requests = [r for r in self.requests if r[0] == new_vid_id]
                    s3 = sum([self.score_for_r(r) for r in new_requests])
                    cs[1].add(new_vid_id)
                    s4 = sum([self.score_for_r(r) for r in new_requests])
                    score_gain = s4 - s3
                    if score_gain <= score_loss:
                        cs[1].remove(new_vid_id)
                        if verbose:
                            print('%d [SB] - ' % j, end='')
                    else:
                        print('%d: YAY ! [%d: %d -> %d] Delta: %d' %
                              (i, cs_id, vid_id, new_vid_id,score_gain - score_loss),
                              end='\n\n' if verbose else '\n')
                        if verbose:
                            print('vid: %d , sg:%d' % (new_vid_id, score_gain))
                        success = True
                        break
                elif verbose:
                    print('%d [TB] - ' % j, end='')
            if not success:
                if verbose:
                    print('ADDING BACK\n\n')
                cs[1].add(vid_id)
