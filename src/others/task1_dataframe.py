import pandas as pd

mydataset = {
  'cars': ["BMW", "Volvo", "Ford"],
  'passings': [3, 7, 2]
}

mydatafrm = pd.DataFrame(mydataset)

print(mydatafrm)
#print(mydatafrm.loc[0])