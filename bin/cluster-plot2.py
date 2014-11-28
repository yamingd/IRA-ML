#!/usr/bin/env python
# -*- coding: utf-8 -*-
# http://stackoverflow.com/questions/9027266/plot-4d-graph-in-python2-7

from mpl_toolkits.mplot3d import Axes3D
import matplotlib.pyplot as plt
plt.rcdefaults()

from matplotlib import cm
import numpy as np
import os

fig = plt.figure(figsize=plt.figaspect(0.5))


def group_cluster(fname):
    cms = {}
    with open(fname) as f:
        for line in f:
            s = line.split('\t')
            cms.setdefault(s[0], [])
            cms[s[0]].append(float(s[-1]) * 1000)
    return cms


def plot_cluster(folder, c, records):
    count = len(records)
    x0 = count / 2
    y0 = records[x0]
    print c, count
    x = range(count)
    x = [xi - x0 for xi in x]
    y = [yi - y0 for yi in records]
    plt.scatter(x, y, alpha=0.5)
    plt.savefig(folder + "/" + str(c) + ".png")
    plt.clf()


def start(fname):
    folder = fname + "-png"
    if not os.path.exists(folder):
        os.mkdir(folder)
    cms = group_cluster(fname)
    print len(cms)
    for c in cms:
        records = cms[c]
        plot_cluster(folder, c, records)

if __name__ == '__main__':
    #start('200-female-clusteredItems')
    #start('400-female-clusteredItems')
    #start('500-female-clusteredItems')
    #start('10-500-female-clusteredItems')
    start('female-750-0001-2')
