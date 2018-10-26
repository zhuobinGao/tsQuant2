import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from statsmodels.graphics.tsaplots import plot_acf,plot_pacf
from arch.unitroot import ADF
from statsmodels.tsa import arima_model
from statsmodels.tsa import stattools
import math
import pandas.tseries


#第7题
# zgsy = pd.read_csv('data/zgsy.csv')
# close = zgsy.iloc[:, 4]
#
# # close.plot()
# # plot_acf(close, lags=20)
# adfclose = ADF(close)
# print(adfclose.summary().as_text())
#
# logReturn = pd.Series(np.log(close)).diff().dropna()
# print(logReturn.head())
# # print(close.head())
#
# adfReturn = ADF(logReturn)
# print(adfReturn.summary().as_text())
# plot_acf(logReturn, lags=20)
# plot_pacf(logReturn, lags=20)
# print('-'*100)
# model1 = arima_model.ARIMA(logReturn.values, order=(0, 0, 1)).fit()
# print('-'*200)
#
#
# model2 = arima_model.ARIMA(logReturn.values, order=(1, 0, 0)).fit()
# print(model1.summary())
# print('='*200)
# print(model2.summary())
#
#
# # plt.show()
# print(close.head())

# 第8题

baiyun = pd.read_csv('data/baiyun.csv')
baiyun.index = pd.to_datetime(baiyun.Date)
# baiyun = baiyun.sort_index();
bclose = baiyun.Close

# print(bclose)

logReturn_all = pd.Series(np.log(bclose)).diff().dropna()
logReturn1 = logReturn_all[-10:]
logReturn = logReturn_all[:-10]
print(logReturn_all.shape,logReturn.shape, logReturn1.shape)
print(logReturn1)
# logReturn.plot()
adfReutn = ADF(logReturn, lags=6)
print(adfReutn.summary().as_text())

# plot_acf(logReturn, lags=20)
# plot_pacf(logReturn, lags=20)

model1 = arima_model.ARIMA(logReturn.values, order=(2, 0, 0)).fit()
model2 = arima_model.ARIMA(logReturn.values, order=(0, 0, 2)).fit()



print("model1.aic=%f \t model2.aic=%f"%(model1.aic, model2.aic))
print(model2.sigma2)
strresid = model2.resid/math.sqrt(model2.sigma2)
# strresid.plot()
plt.subplot(211)
plt.plot(strresid)
# plot_acf(strresid , lags=20)
LinjiuBox = stattools.q_stat(stattools.acf(strresid)[1:13], len(strresid))
print("LinjiuBox:", LinjiuBox[1][-1],'\n',LinjiuBox)
# plt.subplot(212)

print(model2.forecast(10)[0])
print(type(model2.forecast(10)[0]))

test = pd.DataFrame( model2.forecast(10)[0])
test.index = logReturn1.index
test.columns = ['forecast']
test['return1'] = logReturn1.values
logReturn1.columns = ['return1']


print("test:\n",test)

# logReturn1['forecast'] = pd.Series( model2.forecast(10)[0])

# plt.plot(test['forecast'])
plt.subplot(212)
plt.plot(test['return1'])
plt.plot(test['forecast'])



plt.show()


