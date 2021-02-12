import numpy as np
from time import time
from bisect import insort_left
from random import choice, randrange
import sys

INFEASIBLE = 10**8

def transport(supply, demand, cost, tsm=False):
    if sum(supply) < sum(demand):
        return None
    if sum(supply) > sum(demand):
        demand.append(sum(supply) - sum(demand))
        cost = np.append(cost, np.zeros((len(cost), 1)), axis=1)
    supply = np.array(supply, dtype=int)
    demand = np.array(demand, dtype=int)
    m, n = cost.shape
    x = np.NAN * np.ones((m, n), dtype=int)
    crossed_rows = np.zeros(m + 1, dtype=int)
    crossed_cols = np.zeros(n + 1, dtype=int)

    row_best_indices = np.array([list(np.argsort(cost[i])) + [n, n] for i in range(m)])
    col_best_indices = np.array([list(np.argsort(cost[:, j])) + [m, m] for j in range(n)])
    row_first = np.zeros(m, dtype=int)
    row_second = np.ones(m, dtype=int)
    col_first = np.zeros(n, dtype=int)
    col_second = np.ones(n, dtype=int)
    valid_rows = list(range(m))
    valid_cols = list(range(n))
    while True:
        best = -1

        for i in valid_rows:
            first = row_best_indices[i][row_first[i]]
            if row_second[i] >= m:
                rd = 0
            else:
                second = row_best_indices[i][row_second[i]]
                rd = cost[i][second] - cost[i][first]
            if rd > best:
                best = rd
                row, col = i, first

        for j in valid_cols:
            first = col_best_indices[j][col_first[j]]
            if col_second[j] >= n:
                cd = 0
            else:
                second = col_best_indices[j][col_second[j]]
                cd = cost[second][j] - cost[first][j]
            if cd > best:
                best = cd
                row, col = first, j

        if supply[row] <= demand[col]:
            x[row, col] = supply[row]
            demand[col] -= supply[row]
            supply[row] = 0

            crossed_rows[row] = 1
            valid_rows.remove(row)

            for j in valid_cols:
                while crossed_rows[col_best_indices[j][col_first[j]]] == 1:
                    col_first[j] += 1
                    if col_second[j] == col_first[j]:
                        col_second[j] += 1
                while crossed_rows[col_best_indices[j][col_second[j]]] == 1:
                    col_second[j] += 1

            if len(valid_rows) == 1:
                row = valid_rows[0]
                for col in valid_cols:
                    x[row, col] = demand[col]
                    supply[row] -= demand[col]
                    demand[col] = 0
                break
        else:
            x[row, col] = demand[col]
            supply[row] -= demand[col]
            demand[col] = 0

            crossed_cols[col] = 1
            valid_cols.remove(col)

            for i in valid_rows:
                while crossed_cols[row_best_indices[i][row_first[i]]] == 1:
                    row_first[i] += 1
                    if row_second[i] == row_first[i]:
                        row_second[i] += 1
                while crossed_cols[row_best_indices[i][row_second[i]]] == 1:
                    row_second[i] += 1

            if len(valid_cols) == 1:
                col = valid_cols[0]
                for row in valid_rows:
                    x[row, col] = supply[row]
                    demand[col] -= supply[row]
                    supply[row] = 0
                break

    rowbasic = [[] for i in range(m)]
    colbasic = [[] for i in range(n)]
    _x, _y = np.where(~np.isnan(x))
    for ind, row in enumerate(_x):
        rowbasic[row].append(_y[ind])
        colbasic[_y[ind]].append(row)

    uv_time, cyc_time = 0, 0
    while True:
        u = np.NAN * np.ones(m)
        v = np.NAN * np.ones(n)
        uq = []
        vq = []

        uv_time -= time()
        while True:
            # Set an arbitrary value for one u or v:
            unan = np.where(np.isnan(u))[0]
            if len(unan) > 0:
                u[unan[0]] = 0
                uq.append(unan[0])
            else:
                vnan = np.where(np.isnan(v))[0]
                if len(vnan) == 0:
                    break
                v[vnan[0]] = 0
                vq.append(vnan[0])

            while len(uq) > 0 or len(vq) > 0:
                for i in uq:
                    for j in rowbasic[i]:
                        if np.isnan(v[j]):
                            v[j] = cost[i, j] - u[i]
                            vq.append(j)
                uq.clear()
                for j in vq:
                    for i in colbasic[j]:
                        if np.isnan(u[i]):
                            u[i] = cost[i, j] - v[j]
                            uq.append(i)
                vq.clear()
        uv_time += time()

        s = cost - u[:, np.newaxis] * np.ones((m, n)) - v * np.ones((m, n))
        s_min = s.min()
        if s_min >= -1e-9 or s_min > -INFEASIBLE and not tsm:
            break

        cyc_time -= time()

        mins = np.where(s == s_min)
        for p, q in zip(mins[0], mins[1]):
            if len(rowbasic[p]) == 0 or len(colbasic[q]) == 0:
                print('bad pivot', file=sys.stderr)
                continue
            bio = np.zeros((m, n), dtype=int)
            path = [(p, q)]

            def dfs(i, j, direction):
                nonlocal bio, path
                bio[i][j] = 1
                if direction == 0:
                    steps = [(i, nj) for nj in rowbasic[i] if nj != j]
                else:
                    steps = [(ni, j) for ni in colbasic[j] if ni != i]
                for ni, nj in steps:
                    if ni == p and nj == q:
                        return True
                    if bio[ni][nj] == 0:
                        path.append((ni, nj))
                        if dfs(ni, nj, 1 - direction):
                            return True
                        path.pop()
                return False

            insort_left(rowbasic[p], q)
            insort_left(colbasic[q], p)
            dfs(p, q, 0)
            if len(path) > 1:
                break
            print('len path 1 in loop', file=sys.stderr)
            rowbasic[p].remove(q)
            colbasic[q].remove(p)

        if len(path) == 1:
            print('len path 1', file=sys.stderr)
            _x, _y = np.where(~np.isnan(x))
            for ind, i in enumerate(_x):
                j = _y[ind]
                if x[i, j] == 0:
                    rowbasic[i].remove(j)
                    colbasic[j].remove(i)
                    x[i, j] = np.NAN
                    while True:
                        i = randrange(m)
                        j = randrange(n)
                        if np.isnan(x[i, j]):
                            x[i, j] = 0
                            insort_left(rowbasic[i], j)
                            insort_left(colbasic[j], i)
                            break
                    break
            continue

        d = min(x[path[i][0], path[i][1]] for i in range(1, len(path), 2))
        x[p, q] = 0
        if d > 0:
            for i in range(0, len(path), 2):
                x[path[i]] += d
            for i in range(1, len(path), 2):
                x[path[i]] -= d
        i, j = choice([p for p in path if x[p] == 0])
        x[i, j] = np.NAN
        rowbasic[i].remove(j)
        colbasic[j].remove(i)

        cyc_time += time()

    x[np.isnan(x)] = 0
    return x.astype(int)
