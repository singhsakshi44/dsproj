

import pandas as pd
import matplotlib.pyplot as plt

mydf = pd.read_csv("C:/WorkSakshi/Python/data/health/health2.csv")

# data cleanup Strategies

# print(mydf.duplicated())
mydf.drop_duplicates(inplace=True)

# fill data in empty cell with average value
chol_mean = int(mydf["chol"].mean())

mydf.fillna({"chol":chol_mean},inplace=True)

# remove rows with null in key cols
mydf.dropna(subset=['age','sex'],inplace=True)

# Clean wrong data-Either renove it ot fix data
# define rule based on columns

for x in mydf.index:
    if mydf.loc[x,"cp"] > 3:
        mydf.loc[x,'cp'] = 3


# print(mydf.info())
# print(mydf)

# corelation- analysing the data
#print(mydf.corr())
with open("C:/WorkSakshi/Python/data/healthOutput/corrfile.csv", "a") as f:
  f.write(mydf.corr().to_string())


# query
print(mydf.query('age >= 60'))


# Plotting

mydf.plot(kind = 'hist', x = 'age', y = 'chol')
mydf.plot(x = 'age', y = 'chol')
# plt.show()