import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

dir='G:/git_repo/working-set-size-estimation/src/main/resources/benchmarks/filters/'

def load_csv(filename):
    df = pd.read_table(filename)
    return df.values

# 0             1       2           3   4           5       6
# #operation	Real	Real(bytes)	MBF	MBF(bytes)	CCF	CCF(bytes)
# filename = dir + 'twitter/cluster37.0.csv'
# filename = dir + 'concurrency/twitter-8mb.csv'
# filename = dir + 'concurrency/prxy0-test.csv'
filename = 'G:/git_repo/working-set-size-estimation/src/main/benchmarks/' + '20211004/twitter.csv'

X = load_csv(filename)

num_subplots = 2
plt.figure(0, figsize=(16, 2 *4))

# 1. Number
plt.subplot(num_subplots, 1, 1)
plt.plot(X[:,0], X[:,1], lw=1, ls='-', color='k', label='Real')
plt.plot(X[:,0], X[:,3], lw=1, ls='--', color='c', label='MBF')
plt.plot(X[:,0], X[:,5], lw=1, ls=':', color='r', label='CCF')
plt.ticklabel_format(axis="y", style="sci", scilimits=(0,0))
plt.legend(loc='upper left', ncol=2)
plt.ylabel('WSS (Number)')

# 2. Bytes
plt.subplot(num_subplots, 1, 2)
plt.plot(X[:,0], X[:,2], lw=1, ls='-', color='k', label='Real(Bytes)')
plt.plot(X[:,0], X[:,4], lw=1, ls='--', color='c', label='MBF(Bytes)')
plt.plot(X[:,0], X[:,6], lw=1, ls=':', color='r', label='CCF(Bytes)')
plt.ticklabel_format(axis="y", style="sci", scilimits=(0,0))
plt.legend(loc='upper left', ncol=2)
plt.ylabel('WSS (Bytes)')

plt.xlabel('# operation')

plt.savefig(filename.replace("csv", "png"), dpi=300)

# Compute average error
print('Average Error of MBF(Number)', np.divide(X[:,3], X[:,1]).mean()-1.0)
print('Average Error of CCF(Number)', np.divide(X[:,5], X[:,1]).mean()-1.0)

print('Average Error of MBF(Bytes)', np.divide(X[:,4], X[:,2]).mean()-1.0)
print('Average Error of CCF(Bytes)', np.divide(X[:,6], X[:,2]).mean()-1.0)