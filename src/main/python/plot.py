import matplotlib.pyplot as plt
import pandas as pd

dir='G:/git_repo/working-set-size-estimation/src/main/resources/benchmarks/filters/'

def load_csv(filename):
    df = pd.read_table(filename)
    return df.values

# 0             1       2           3   4           5       6
# #operation	Real	Real(bytes)	MBF	MBF(bytes)	CCF	CCF(bytes)
# X = load_csv(dir + 'MinorAgingRandomBench.csv')
# X = load_csv(dir + 'MinorAgingSequencialBench.csv')
# X = load_csv(dir + 'SequencialBench2.csv')
# X = load_csv(dir + 'RandomBench-c4.csv')
# X = load_csv(dir + 'msr/prxy0.csv')
X = load_csv(dir + 'twitter/cluster37.0.csv')

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

plt.savefig(dir + 'twitter/cluster37.0.png', dpi=300)
