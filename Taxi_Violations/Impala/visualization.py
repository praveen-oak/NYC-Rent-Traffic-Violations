import pandas as pd
from sklearn.preprocessing import MinMaxScaler
import matplotlib.pyplot as plt

def plot(borough):
    df = pd.read_csv(borough + '.csv', header=None, names=['Date', 'Borough', 'TaxiCount', 'ViolationsCount'])
    scaler = MinMaxScaler()
    x = df[['TaxiCount', 'ViolationsCount']].values
    x_scaled = scaler.fit_transform(x)
    df[['TaxiCount']] = pd.Series(x_scaled[:,0])
    df[['ViolationsCount']] = pd.Series(x_scaled[:,1])
    plt.figure(figsize=(7,7))
    plt.plot(df['Date'], df['TaxiCount'], label='TaxiCount')
    plt.plot(df['Date'], df['ViolationsCount'], label='ViolationsCount')
    plt.xticks(rotation=90)
    plt.legend()
    plt.savefig(borough + '.png')
    plt.close()

plot('manhattan')
plot('brooklyn')
plot('bronx')
plot('queens')
plot('staten_island')
