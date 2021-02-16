from collections import defaultdict
import matplotlib.pyplot as plt

for name in ('Cost', 'Successful reqs.', 'Failed reqs.',\
        'Violated RT reqs.', 'Violated reliability reqs.'):
    res = defaultdict(float)
    cnt = defaultdict(int)
    with open(name + '.txt') as f:
        for line in f:
            alg, broker, val = line.split(',')
            res[(alg, broker)] += float(val)
            cnt[(alg, broker)] += 1
    positions = []
    names = []
    heights = []
    for i, broker in enumerate(['single broker', 'user&service balancing',\
            'user balancing', 'service balancing', 'no balancing']):
        for j, alg in enumerate(['Random', 'Round Robin', 'Greedy', 'AP', "TP"]):
            positions.append(i * 6 + j)
            heights.append(res[(alg, broker)] / cnt[(alg, broker)])
            names.append(alg)
    plt.bar(positions, heights, align='center')
    plt.xticks(positions, names, rotation=45)
    plt.title(name)
    plt.show()
    plt.savefig(name + '.pdf', format='pdf')
