import os
import sys
from datetime import datetime

from python_solver import World

power = float(sys.argv[1]) if len(sys.argv) == 2 else 0
print('got power = %f' % power)

score = 0
for filename in os.listdir('input'):
    print('\n\n' + str(datetime.now()))
    w = World('input/%s' % filename)
    score += w.algo_vid(power)
    w.write_solution(prefix='power-%.2f' % power)
print(score)
