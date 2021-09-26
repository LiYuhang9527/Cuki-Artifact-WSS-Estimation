import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

file='G:/datasets/msr-cambridge1/prxy_0.csv'

def load_csv(filename):
    df = pd.read_csv(filename, header=None, nrows=1024*1000)
    df = df.reset_index()
    return df.to_numpy()

X = load_csv(file)

num_subplots = 2
plt.figure(0, figsize=(16, 2 *4))

# 1. Object
plt.subplot(num_subplots, 1, 1)
plt.scatter(X[:,0], X[:,5], s=0.01, color='k', label='Offset')
plt.ticklabel_format(axis="y", style="sci", scilimits=(0,0))
plt.legend(loc='upper left', ncol=2)
plt.ylabel('Offset')

# 2. Size
plt.subplot(num_subplots, 1, 2)
plt.scatter(X[:,0], X[:,6], s=0.01, color='k', label='Size(Bytes)')
plt.ticklabel_format(axis="y", style="sci", scilimits=(0,0))
plt.legend(loc='upper left', ncol=2)
plt.ylabel('Size (Bytes)')

plt.xlabel('time')

pngfile = file.replace('csv', 'png')
plt.savefig(pngfile, dpi=300)
