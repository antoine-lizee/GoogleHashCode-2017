import os
from collections import deque
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
            self.video_sizes = [int(v) for v in next(f).strip().split(' ')]
            self.endpoint_latencies_gains = []
            for i in range(self.n_E):
                dc_latency, n_cs = (int(v) for v in next(f).strip().split(' '))
                latency_gains = {}
                for k in range(n_cs):
                    id, lat = (int(v) for v in next(f).strip().split(' '))
                    latency_gains[id] = dc_latency - lat
                self.endpoint_latencies_gains.append(latency_gains)
            self.tot_requests = 0
            self.requests = []
            for i in range(self.n_R):
                video_id, endpoint_id, n_requests = (int(v) for v in next(f).strip().split(' '))
                self.requests.append([video_id, endpoint_id, n_requests])
                self.tot_requests += n_requests
        self.init_cache_servers()
        # Optim:
        self.init_lookup_objects()

    def init_cache_servers(self):
        self.total_score = 0
        self.cache_servers = [[self.cs_capacity, set()] for i in range(self.n_C)]
        self.cs_for_vid = [set() for i in range(self.n_V)]

    def set_cache_server_to(self, cs_id, vids):
        for vid in self.cache_servers[cs_id][1]:
            self.cs_for_vid[vid].remove(cs_id)

        self.cache_servers[cs_id] = [
            self.cs_capacity - sum(self.video_sizes[v] for v in vids),
            vids,
            ]
        for vid in vids:
            self.cs_for_vid[vid].add(cs_id)

    def init_lookup_objects(self):
        self.req_ids_for_vid = [set() for i in range(self.n_V)]
        self.req_ids_for_e = [set() for i in range(self.n_E)]
        for i, r in enumerate(self.requests):
            self.req_ids_for_vid[r[0]].add(i)
            self.req_ids_for_e[r[1]].add(i)
        self.e_for_cs = [{} for i in range(self.n_C)]
        for i, gains in enumerate(self.endpoint_latencies_gains):
            for cs_id, lat_gain in gains.items():
                self.e_for_cs[cs_id][i] = lat_gain
        self.sorted_video_ids = sorted(range(self.n_V), key=lambda id: self.video_sizes[id])
        self.sorted_video_sizes = sorted(self.video_sizes)
        self.sorted_video_indexes = [0] * self.n_V
        for i, id in enumerate(self.sorted_video_ids):
            self.sorted_video_indexes[id] = i

    def req_ids_for_cs(self, cs_id):
        reqs = set()
        for e in self.e_for_cs[cs_id].keys():
            reqs |= self.req_ids_for_e[e]
        return reqs

    def add_vid_to_cs(self, vid_id, cs_id):
        if cs_id is not None:
            cs = self.cache_servers[cs_id]
            size = self.video_sizes[vid_id]
            if size <= cs[0]:
                cs[0] -= size
                cs[1].add(vid_id)
                self.cs_for_vid[vid_id].add(cs_id)
                return True
        return False

    def write_solution(self, prefix=''):
        filename = 'output/%s_%.0f_%s_%s.txt' % (self.filename, self.score(), prefix, datetime.datetime.now().isoformat())
        cs = []
        for i, (cap, vids) in enumerate(self.cache_servers):
            if vids:
                cs.append('%d %s' % (i, ' '.join((str(v) for v in vids))))
        with open(filename, 'w') as f:
            f.write(str(len(cs)) + '\n')
            for line in cs:
                f.write(line + '\n')

    def load_solution(self, filename):
        self.init_cache_servers()
        with open(filename, 'r') as f:
            first_line = next(f).strip()
            n_C = int(first_line)
            for i in range(n_C):
                values = next(f).strip().split(' ')
                cs_id = int(values[0])
                vids = set(int(v) for v in values[1:])
                self.set_cache_server_to(cs_id, vids)
        print('Loaded %d caches servers for a solution worth %d' % (n_C, self.score()))

    def score(self):
        tot_score = 0.0
        tot = 0.0
        for (video_id, endpoint_id, n_requests) in self.requests:
            gains = self.endpoint_latencies_gains[endpoint_id]
            applicable_gains = [0]
            for cs_id, gain in gains.items():
                if video_id in self.cache_servers[cs_id][1]:
                    applicable_gains.append(gain)
            tot_score += max(applicable_gains) * n_requests
            tot += n_requests
        return tot_score / tot * 1000

    def best_lat_gain_for_e(self, endpoint_id, video_id):
        size = self.video_sizes[video_id]
        max_gain = 0
        best_cs = None
        for cs_id, gain in self.endpoint_latencies_gains[endpoint_id].items():
            if self.cache_servers[cs_id][0] > size:
                if max_gain < gain:
                    best_cs = cs_id
                    max_gain = gain
        return best_cs, max_gain

    def util(self, r):
        return self.best_lat_gain_for_e(r[1], r[0])[1] * r[2] # * (self.video_sizes[r[0]]) ** (2)

    def algo(self, n):
        self.init_cache_servers()
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
        endpoint_gains = self.endpoint_latencies_gains[r[1]]
        applicable_gains = [0]
        for cs_id, lat in endpoint_gains.items():
            if r[0] in self.cache_servers[cs_id][1]:
                applicable_gains.append(lat)
        return max(applicable_gains) * r[2] / self.tot_requests * 1000

    def total_score_for_request_ids(self, request_ids):
        return sum(self.score_for_r(self.requests[r_id]) for r_id in request_ids)

    def greedy(self, n, m, verbose=True):
        # Setup derivative estimation objects
        width = 50
        delta_scores = deque()
        s_delta_scores = 0
        delta_is = deque()
        s_delta_is = 0
        n_j = []
        last_i = -1
        dd = 0
        # Main loop
        for i in range(n):
            # Pick cache server at random and vid within it.
            cs_id = randint(0, self.n_C - 1)
            cs = self.cache_servers[cs_id]
            vid_id = choice(tuple(cs[1]))
            request_ids = self.req_ids_for_vid[vid_id]
            size = self.video_sizes[vid_id]
            # Compute score loss by removing it
            if verbose:
                print('REMOVING vid %d from %d' % (vid_id, cs_id))
            s1 = self.total_score_for_request_ids(request_ids)
            cs[1].remove(vid_id)
            s2 = self.total_score_for_request_ids(request_ids)
            score_loss = s1 - s2
            if verbose:
                print('sl: %d' % score_loss)
            success = False
            # Try to add another video
            for j in range(m):
                new_vid_id = randint(0, self.n_V - 1)
                if self.video_sizes[new_vid_id] < size and new_vid_id not in cs[1]:
                    new_request_ids = self.req_ids_for_vid[new_vid_id]
                    if not new_request_ids:
                        if verbose:
                            print('%d [unwanted vid] - ' % j, end='')
                        continue
                    s3 = self.total_score_for_request_ids(new_request_ids)
                    cs[1].add(new_vid_id)
                    s4 = self.total_score_for_request_ids(new_request_ids)
                    score_gain = s4 - s3
                    if score_gain <= score_loss:
                        cs[1].remove(new_vid_id)
                        if verbose:
                            print('%d [no gain] - ' % j, end='')
                    else:
                        success = True
                        # Try another video ?
                        for j in range(m):
                            another_vid_id = randint(0, self.n_V - 1)
                            if self.video_sizes[another_vid_id] < size and another_vid_id not in cs[1]:
                                another_vid_request_ids = self.req_ids_for_vid[another_vid_id]
                                if not another_vid_request_ids:
                                    continue
                                s5 = self.total_score_for_request_ids(new_request_ids)
                                cs[1].add(new_vid_id)
                                s6 = self.total_score_for_request_ids(new_request_ids)
                                score_addition = s6 - s5
                        # Update derivative estimation
                        d_s = score_gain - score_loss
                        s_delta_scores += d_s
                        delta_scores.append(d_s)
                        d_i = i - last_i
                        last_i = i
                        s_delta_is += d_i
                        delta_is.append(d_i)
                        n_j.append(j)
                        if len(delta_scores) > width:
                            s_delta_is -= delta_is.popleft()
                            s_delta_scores -= delta_scores.popleft()
                        dd = s_delta_scores / s_delta_is
                        # Print out
                        print('%4d/%2i: %.2f (Deltas: %d|%d ) [%d: %d -> %d] ' %
                              (i, j, dd, d_s, d_i, cs_id, vid_id, new_vid_id),
                              end='\n\n' if verbose else '\n')
                        if verbose:
                            print('vid: %d , sg:%d' % (new_vid_id, score_gain))
                        break
                elif verbose:
                    print('%d [bad vid] - ' % j, end='')
            if not success:
                if verbose:
                    print('ADDING BACK\n\n')
                cs[1].add(vid_id)

    def best_cs_for_vid(self, vid_id):
        size = self.video_sizes[vid_id]
        req_ids = self.req_ids_for_vid[vid_id]
        cs_gains = [0] * self.n_C
        for req_id in req_ids:
            r = self.requests[req_id]
            lat_gains = self.endpoint_latencies_gains[r[1]]
            existing_gain = max((lat_gains.get(c, 0) for c in self.cs_for_vid[vid_id]), default=0)
            for cs_id in range(self.n_C):
                cs = self.cache_servers[cs_id]
                if vid_id in cs[1] or cs[0] < size:
                    continue
                req_cs_gain = (lat_gains.get(cs_id, 0) - existing_gain) * r[2]
                if req_cs_gain > 0:
                    cs_gains[cs_id] += req_cs_gain
        return max(enumerate(cs_gains), key=lambda x: x[1])

    def algo_vid_optim(self, steps, n_max):
        for i in range(steps):
            # Optimal loop
            best_gain_for_vid = []
            best_cs_for_vid = []
            for vid_id in range(self.n_V):
                best_cs_id, best_cs_gain = self.best_cs_for_vid(vid_id)
                best_gain_for_vid.append(best_cs_gain)
                best_cs_for_vid.append(best_cs_id)
            best_gains = sorted(enumerate(best_gain_for_vid), key=lambda x: -x[1])
            best_vid_id, best_gain = best_gains[0]
            if best_gain == 0:
                return
            for n in range(n_max):
                best_vid_id, best_gain = best_gains[n]
                if best_gain == 0:
                    break
                self.add_vid_to_cs(best_vid_id, best_cs_for_vid[best_vid_id])
                self.total_score += best_gain / self.tot_requests * 1000
                print('vid:%d cs:%d score_gain:%d' %
                      (best_vid_id, best_cs_for_vid[best_vid_id], best_gain / self.tot_requests * 1000))
            if i % 10 == 0 or i == steps-1:
                print(sum(cs[0] for cs in self.cache_servers) / self.n_C)
                print(self.total_score)

    def algo_vid(self, power_0=1, power_1=None, max_steps=None):
        if max_steps is None:
            max_steps = self.n_V * 5
        power_1 = power_1 or power_0
        power = power_0
        # First Optimal loop
        best_gain_for_vid = []
        best_cs_for_vid = []
        for vid_id in range(self.n_V):
            best_cs_id, best_cs_gain = self.best_cs_for_vid(vid_id)
            best_gain_for_vid.append(best_cs_gain)
            best_cs_for_vid.append(best_cs_id)
        for i in range(max_steps):
            # Get best vid/cs couple and add it
            best_vid_id, best_gain = max(enumerate(best_gain_for_vid),
                                         key=lambda x: x[1] / self.video_sizes[x[0]] ** power)
            if best_gain == 0:
                print('STOPPING because breaking')
                break
            has_added = self.add_vid_to_cs(best_vid_id, best_cs_for_vid[best_vid_id])
            if has_added:
                self.total_score += best_gain / self.tot_requests * 1000
                # print('vid:%d cs:%d size:%d score_gain:%d' %
                #       (best_vid_id, best_cs_for_vid[best_vid_id], self.video_sizes[best_vid_id], best_gain / self.tot_requests * 1000))
            # else:
            #     print('vid:%d cs:%d - reloading...' % (best_vid_id, best_cs_for_vid[best_vid_id]))
            if i % 10 == 0:
                avg_server_size = sum(cs[0] for cs in self.cache_servers) / self.n_C
                print('%5d: %6d server size left: %.1f (power=%.3f)' %
                      (i, self.total_score, avg_server_size, power))
                power = power_1 - avg_server_size / self.cs_capacity * (power_1 - power_0)
            # Update main table
            best_cs_for_vid[best_vid_id], best_gain_for_vid[best_vid_id] = self.best_cs_for_vid(best_vid_id)
        print('%5d: %6d server size left: %.1f' %
              (i, self.total_score, sum(cs[0] for cs in self.cache_servers) / self.n_C))
        return self.total_score

    def vid_gains_for_cs(self, cs_id):
        vid_gains = [0] * self.n_V
        for vid_id in range(self.n_V):
            req_ids = self.req_ids_for_vid[vid_id]
            for req_id in req_ids:
                r = self.requests[req_id]
                lat_gains = self.endpoint_latencies_gains[r[1]]
                existing_gain = max((lat_gains.get(c, 0) for c in self.cs_for_vid[vid_id] - {cs_id}), default=0)
                req_cs_gain = (lat_gains.get(cs_id, 0) - existing_gain) * r[2]
                if req_cs_gain > 0:
                    vid_gains[vid_id] += req_cs_gain
        return vid_gains

    def current_optim_vid_set_for_cs(self, cs_id, power=1):
        vgs = self.vid_gains_for_cs(cs_id)
        full_vgs = [
            vg + (self.video_sizes[vg[0]], vg[1] / self.video_sizes[vg[0]] ** power) for vg in enumerate(vgs)
            if vg[1] > 0
            ]
        mem = self.cs_capacity
        current_vids = self.cache_servers[cs_id][1]
        optimal_vids = set()
        optimal_gain = 0
        for i, fvg in enumerate(sorted(full_vgs, reverse=True, key=lambda x: x[3])):
            if mem >= fvg[2]:
                mem -= fvg[2]
                optimal_vids.add(fvg[0])
                optimal_gain += fvg[1]
                if mem == 0:
                    # print('breaking at %dth video seen, added %d in total' % (i, len(optimal_vids)))
                    break
        if current_vids ^ optimal_vids:
            current_gain = sum(vgs[v_id] for v_id in current_vids)
            if current_gain < optimal_gain:
                return (optimal_gain - current_gain) / self.tot_requests * 1000, optimal_vids
        return 0, set()

    def greedy_vid(self, power=1, steps=400):
        cs_gains = []
        print('Computing initial optimal vid for cs matrix...')
        for cs_id in range(self.n_C):
            cs_gains.append(self.current_optim_vid_set_for_cs(cs_id, power))
        print('Done. Starting iterating...')
        for i in range(steps):
            max_cs_id, cs_gain = max(enumerate(cs_gains), key=lambda x: x[1][0])
            if cs_gain[0] == 0:
                print('No more to be gained...')
                break
            update = self.current_optim_vid_set_for_cs(max_cs_id, power)
            if cs_gain[0] != update[0]:
                print('Stale result, continuing...')
                cs_gains[max_cs_id] = update
                continue
            print('Resetting cs %d at optimal point (gain %d)' % (max_cs_id, cs_gain[0]))
            self.set_cache_server_to(max_cs_id, cs_gain[1])
            cs_gains[max_cs_id] = self.current_optim_vid_set_for_cs(max_cs_id, power)
