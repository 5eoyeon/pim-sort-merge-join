import pandas as pd
import numpy as np

num_rows = 1000000
num_cols = 4

data = np.random.randint(1, num_rows * 3, size=(num_rows, num_cols - 1))

unique_col = np.random.choice(range(1, (num_rows * 3 + 1)), size=num_rows, replace=False)

data = np.column_stack((unique_col, data))

df = pd.DataFrame(data, columns=[f"col{i+1}" for i in range(num_cols)])

df.to_csv('data_100.csv', index=False)


data = np.random.randint(1, num_rows * 3, size=(num_rows, num_cols - 1))

unique_col = np.random.choice(range(1, (num_rows * 3 + 1)), size=num_rows, replace=False)

data = np.column_stack((unique_col, data))

df = pd.DataFrame(data, columns=[f"col{i+1}" for i in range(num_cols)])

df.to_csv('data_100(1).csv', index=False)