def f():
    x = [[1,2],[5,2]]
    y = 3
    return x, y

x, y = f()

import pandas as pd

data = {'A':[1, 1, 2, 3, 3, 3],
        'B':[4, 4, 5, 6, 6, 6]}

DFrame = pd.DataFrame(data)
group = DFrame.groupby(by=['A']).first().reset_index()
print(group)