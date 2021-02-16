from collections import defaultdict, OrderedDict
import matplotlib.pyplot as plt

plt.rcParams.update({'font.size': 18})
plt.figure(figsize=(20,10))
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
    name_positions = []
    heights = []
    #colors = []
    colors = ['black', 'purple', 'green', 'blue', 'red']
    labels = []
    for i, broker in enumerate(['single broker', 'no balancing',\
            'user balancing', 'service balancing', 'user&service balancing']):
        #colors.extend(['black', 'purple', 'green', 'blue', 'red'])
        for j, alg in enumerate(['Random', 'Round Robin', 'Greedy', 'AP', "TP"]):
            positions.append(i * 6 + j)
            heights.append(res[(alg, broker)] / cnt[(alg, broker)])
            #labels.append(alg)
            plt.bar(i * 6 + j, res[(alg, broker)] / cnt[(alg, broker)], align='center',
                    color= colors[j], label=alg + ' Selection')
        names.append(broker)
        name_positions.append(i * 6 + 2)
    #plt.bar(positions, heights, align='center', color=colors, label=labels)
    plt.xticks(name_positions, names)  #, rotation=45)
    plt.title(name.replace('Cost', "Average cost"))
    handles, labels = plt.gca().get_legend_handles_labels()
    by_label = OrderedDict(zip(labels, handles))
    plt.legend(by_label.values(), by_label.keys())
    #plt.legend()
    #plt.show()
    plt.savefig(name + '.pdf', format='pdf')
    plt.clf()
    #exit(0)
