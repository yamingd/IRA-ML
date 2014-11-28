#!/usr/bin/env python
# -*- coding: utf-8 -*-
# http://stackoverflow.com/questions/9027266/plot-4d-graph-in-python2-7

from mpl_toolkits.mplot3d import Axes3D
import matplotlib.pyplot as plt
plt.rcdefaults()

from matplotlib import cm
import numpy as np


def group_cluster(fname):
    cms = {}
    with open(fname) as f:
        for line in f:
            s = line.split('\t')
            cms.setdefault(s[0], [])
            cms[s[0]].append(map(float, s[1:]))
    return cms


def distance(vec1, vec2):
    c1 = 0
    for i in xrange(len(vec1)):
        if vec1[i] != vec2[i]:
            c1 += 1
    return c1


def cluster_distance(records):
    c = len(records)
    ds = []
    for i in xrange(c):
        item1 = records[i]
        for j in xrange(i + 1, c):
            item2 = records[j]
            d = distance(item1, item2)
            ds.append(d)
    dm = {}
    for d in ds:
        dm.setdefault(d, 0)
        dm[d] += 1
    return dm


def plot_cluster(cluster_id, records):
    x = []
    y = []
    s1 = []
    s2 = []
    for item in records:
        x.append(item[0])
        y.append(item[3])
        s1.append(item[1])
        s2.append(item[2])
    #x = np.array(x)
    #y = np.array(y)
    #s1 = np.array(s1)
    #s2 = np.array(s2)

    fig = plt.figure(figsize=plt.figaspect(0.5))
    ax = fig.add_subplot(1, 2, 1, projection='3d')

    ax.scatter(x, y, s2, marker='o')
    ax.set_xlabel('body')
    ax.set_ylabel('blod')
    ax.set_zlabel('height')

    ax = fig.add_subplot(1, 2, 2, projection='3d')

    ax.scatter(x, y, s1, marker='o')
    ax.set_xlabel('body')
    ax.set_ylabel('blod')
    ax.set_zlabel('age')

    """
    x, y = np.meshgrid(x, y)  # <-- returns a 2D grid from initial 1D arrays
    surf = ax.plot_surface(x, y, s1, rstride=1, cstride=1)
    #ax.set_zlim3d(-1.01, 1.01)

    #fig.colorbar(surf, shrink=0.5, aspect=10)
    """


def start(fname):
    cms = group_cluster(fname)
    cmd = {}
    rsum = 0.0
    for c in cms:
        records = cms[c]
        ds = cluster_distance(records)
        cmd[c] = ds
        total = sum(ds.values())
        r1 = 100.0 * (ds.get(0, 0) + ds.get(1, 0)) / total
        print c, len(records), ds, r1
        rsum += r1
        """
        print len(records)
        if len(records) < 1000:
            plot_cluster(c, records)
            plt.show()
            break
        """
    print rsum / len(cms)

if __name__ == '__main__':
    #start('clusterUserInfo-female')
    start('cluster-figure-female')
