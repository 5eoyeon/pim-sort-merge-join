import pandas as pd
import numpy as np

num_rows = 300000
num_cols = 4

data = np.random.randint(1, 1000000, size=(num_rows, num_cols - 1))

unique_col = np.random.choice(range(1, num_rows + 1), size=num_rows, replace=False)

data = np.column_stack((unique_col, data))

df = pd.DataFrame(data, columns=[f"col{i+1}" for i in range(num_cols)])

df.to_csv('test_data(1).csv', index=False)