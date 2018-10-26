import pandas as pd
import matplotlib.pyplot as plt
from statsmodels.graphics.tsaplots import *
from statsmodels.tsa import stattools
from arch.unitroot import ADF

CRSP = pd.read_csv('data/CRSPday.csv')
SP500 = pd.read_csv('data/SP500.csv')
# 第5题
#
# ibm = CRSP.ibm
# ibm.plot()
# # plot_acf(ibm, use_vlines=True, lags=30)
# plot_acf(ibm, use_vlines=True, lags=20)
# adfIBM = ADF(ibm)
# print(adfIBM.summary().as_text())
# LinjiuBox = stattools.q_stat(stattools.acf(ibm)[1:13], len(ibm))
# print(LinjiuBox[1][-1])


# 第6题
# ge = CRSP.iloc[:,3]
#
# ge.plot()
# plot_acf(ge, use_vlines=True, lags=20)
# LinjiuBox = stattools.q_stat(stattools.acf(ge)[1:2], len(ge))
# print('lag=2',LinjiuBox[1][-1])
# LinjiuBox = stattools.q_stat(stattools.acf(ge)[1:9], len(ge))
# print('lag=9',LinjiuBox[1][-1])


# 第7题

r500 = SP500.r500
print(r500.head())

plt.subplot(221)
r500.plot(subplots=True)




plot_acf(r500, lags=20)
plot_pacf(r500, lags=20)
adfIBM = ADF(r500)
print(adfIBM.summary().as_text())




plt.show()