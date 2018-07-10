# -*- coding: utf-8 -*-
"""
Created on Sat Jun 16 14:32:55 2018

@author: Antoine
"""
import os
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd

#%%==========================================================================
#  Data import
#============================================================================

# Datafile's root
racines = {
        'C:\\Users\\antoine\\Desktop\\Polytechnique\\Binet\\X Finance\\Cryptocurrencies exchange rate' :
            'C:/Users/antoine/Desktop/Polytechnique/Binet/X Finance/Cryptocurrencies exchange rate/Data/'
           }

racine = racines[os.getcwd ()]

# Choice of the crypto
i = 0
pairs = ['XXBTZEUR','XETHZEUR','XLTCZEUR','BCHEUR','DASHEUR']
pair = pairs[i]

data = pd.read_csv(open(racine + 'Data_' + pair + '/' + pair + '_complete_candle_24h.csv'), index_col=0)

data = data[['pClose', 'previousClose','V','previousV','return','returnV']].iloc[100:]

# remove absurd data
data['returnV'][abs(data['returnV']) > 5] = 0

def switchBack(df, k=1):
    df.index = df.index + k
    return df.iloc[:-k]

variables = pd.DataFrame()
variables['return'] = switchBack(pd.DataFrame.copy(data['return']), 1)
variables['returnV'] = switchBack(pd.DataFrame.copy(data['returnV']), 1)

# normalization
variables['return'] = (variables['return']-variables['return'].mean())/variables['return'].std()
variables['returnV'] = (variables['returnV']-variables['returnV'].mean())/variables['returnV'].std()

isUp = data['return'] >= 0
up = data[isUp]
down = data[~isUp]

#%%==========================================================================
#  Plot 2D
#============================================================================
plt.scatter(variables['return'][isUp],variables['returnV'][isUp],color='r', label='Positive return')
plt.scatter(variables['return'][~isUp],variables['returnV'][~isUp],color='b', label='Negative return')

plt.xlabel('Previous return')
plt.ylabel('Previous return_Vol')

plt.legend()

#%%==========================================================================
#   Plot 3D
#============================================================================
variables['return2'] = switchBack(pd.DataFrame.copy(data['return']), 2)
variables['return2'] = (variables['return2']-variables['return2'].mean())/variables['return2'].std()

variables = variables.iloc[1:]
fig = plt.figure()
ax = fig.gca(projection='3d')

ax.scatter(variables['return'][isUp], variables['return2'][isUp], variables['returnV'][isUp],color='r', label='Positive return')
ax.scatter(variables['return'][~isUp], variables['return2'][~isUp], variables['returnV'][~isUp], color='b', label='Negative return')

ax.set_xlabel('Previous return')
ax.set_ylabel('Previous previous return')
ax.set_zlabel('Previous return_Vol')

plt.legend()