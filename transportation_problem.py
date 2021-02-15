import numpy as np
from time import time
from bisect import insort_left
from random import choice, randrange
import sys
from util import INFEASIBLE

def transport(supply, demand, cost):
    if sum(supply) < sum(demand):
        supply.append(sum(demand) - sum(supply))
        cost = np.append(cost, INFEASIBLE * np.ones((1, len(demand))), axis=0)
    if len(supply) == 1:
        return np.array([demand])
    if sum(supply) > sum(demand):
        demand.append(sum(supply) - sum(demand))
        cost = np.append(cost, np.zeros((len(cost), 1)), axis=1)
    if len(demand) == 1:
        return np.array([supply]).transpose()
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

    x[np.isnan(x)] = 0
    return x.astype(int)
