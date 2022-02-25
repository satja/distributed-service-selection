from collections import defaultdict, OrderedDict
import numpy as np

def get_label(name):
    if name != 'single broker':
        name = 'multi-broker + ' + name
        name.replace('&', ' & ')
    return name

def ff(x):
    if isinstance(x, str):
        return x
    return '{:.3f}'.format(x)

table = '\
\\begin{table}[h]\n\
\\color{red}\n\
\\caption{CAPTION}\n\
    \\centering\n\
    \\resizebox{\columnwidth}{!}{\n\
        \\begin{tabular}{|c||c|c|c|c|c|}\n\
            \\hline\n\
            Selection & single & multi-broker + & multi-broker + & multi-broker + & multi-broker + \\\\ \n\
            procedure & broker & no balancing & user balancing & service balancing & u\&s balancing \\\\ \n\
            \\hline \\hline\n\
            CONTENT\
        \\end{tabular}\n\
    }\n\
    \\label{table:LABEL}\n\
\\end{table}'

def shorten(name):
    if name.startswith('Failed'):
        return 'dropped'
    if 'reliab' in name:
        return 'rel'
    if 'RT' in name:
        return 'rt'
    if 'Succ' in name:
        return 'succ'
    if 'time' in name:
        return 'time'
    if 'Cost' in name:
        return 'cost'
    if 'brokers' in name:
        return 'high-load-brokers'
    if 'services' in name:
        return 'high-load-services'

def capt(name):
    if name == 'dropped':
        return 'Dropped requests / total number of requests'
    if name == 'rel':
        return 'Requests with violated reliability requirements / total number of requests'
    if name == 'rt':
        return 'Requests with violated RT requirements / total number of requests'
    if name == 'succ':
        return 'Requests with satisfied QoS requirements / total number of requests'
    if name == 'time':
        return 'Average time of the broker selection procedure (s)'
    if name == 'cost':
        return 'Average cost per request'
    if name == 'high-load-brokers':
        return 'Average percentage of brokers under high load'
    if name == 'high-load-services':
        return 'Average percentage of services under high load'
    return name

for name in ('Successful reqs.', 'Failed reqs.', 'Cost', 'Violated reliability reqs.',
        'Violated RT reqs.', 'Avg. selection time', 'High-load-brokers', 'High-load-services'):
    res = defaultdict(float)
    cnt = defaultdict(int)
    with open(name + '.txt') as f:
        for line in f:
            alg, broker, val = line.split(',')
            res[(alg, broker)] += float(val)
            cnt[(alg, broker)] += 1
    lines = []
    name = shorten(name)
    for i, alg in enumerate(['Random', 'Round Robin', 'Greedy', 'AP', "TP"]):
        line = []
        for j, broker in enumerate(['single broker', 'no balancing',\
                'user balancing', 'service balancing', 'user&service balancing']):
            if not cnt[(alg, broker)]:
                if name == 'time':
                    line.append('$>$ 2.000')
                else:
                    line.append('N/A')
            else:
                line.append(res[(alg, broker)] / cnt[(alg, broker)])
        best = min([x for x in line if isinstance(x, float)])
        if name == 'succ':
            best = max([x for x in line if isinstance(x, float)])
        j = line.index(best)
        line[j] = '{' + '\\bf {:.3f}'.format(line[j]) + '}'
        q = list(map(ff, [alg] + line))
        lines.append(' & '.join(q) + ' \\\\ \hline')
    t = table.replace('CONTENT', '\n'.join(lines)).replace('LABEL', name).replace('CAPTION', f"{capt(name)}")
    with open(f'../Dropbox/fse-paper/revision/tables/{name}.tex', 'w') as g:
        g.write(t)
