import numpy as np
import pandas as pd

num_rows = 1000
num_cols = 4

data = np.random.randint(0, num_rows, size=(num_rows, num_cols))
df = pd.DataFrame(data, columns=[f'col{i}' for i in range(num_cols)])

df.to_csv('test_data(2).csv', index=False)
