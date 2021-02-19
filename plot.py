from collections import defaultdict, OrderedDict
import matplotlib.pyplot as plt
import numpy as np

def get_label(name):
    if name != 'single broker':
        name = 'multi-broker + ' + name
        name.replace('&', ' & ')
    return name

plt.rcParams.update({'font.size': 18})
#plt.figure(figsize=(20,10))
for name in ('Cost', 'Successful reqs.', 'Failed reqs.',\
        'Violated RT reqs.', 'Violated reliability reqs.', 'Avg. selection time'):
    plt.figure(figsize=(20,10))
    res = defaultdict(float)
    cnt = defaultdict(int)
    with open(name + '.txt') as f:
        for line in f:
            alg, broker, val = line.split(',')
            res[(alg, broker)] += float(val)
            cnt[(alg, broker)] += 1
    positions = []
    names = []
    name_positions = []
    heights = []
    #colors = []
    colors = ['black', 'purple', 'green', 'blue', 'red']
    labels = []
    plt.grid()
    for i, alg in enumerate(['Random', 'Round Robin', 'Greedy', 'AP', "TP"]):
        for j, broker in enumerate(['single broker', 'no balancing',\
                'user balancing', 'service balancing', 'user&service balancing']):
            positions.append(i * 6 + j)
            if (alg, broker) not in res:
                heights.append(0)
            else:
                heights.append(res[(alg, broker)] / cnt[(alg, broker)])
            plt.bar(i * 6 + j, heights[-1], align='center',
                    color= colors[j], label=get_label(broker))
        names.append(alg + ' Selection')
        name_positions.append(i * 6 + 2)
    if .1 < max(heights) < 1:
        plt.yticks(np.arange(0,max(heights)+0.05,0.05))
    #plt.bar(positions, heights, align='center', color=colors, label=labels)
    plt.xticks(name_positions, names)  #, rotation=45)
    title = name.replace('Cost', "Average cost").replace('Succ', 'QoS-succ')
    if 'time' in title:
        title += ' (ms)'
    #else:
    #    title += ' (%)'
    plt.title(title)
    handles, labels = plt.gca().get_legend_handles_labels()
    by_label = OrderedDict(zip(labels, handles))
    plt.legend(by_label.values(), by_label.keys())
    #plt.show()
    plt.savefig(name + '.pdf', format='pdf')
    plt.clf()
