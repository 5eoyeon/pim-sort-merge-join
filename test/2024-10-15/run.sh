make

echo "10000"
./cpu_app ./data/data_1.csv './data/data_1(1).csv'
./app ./data/data_1.csv './data/data_1(1).csv'

echo "100000"
./cpu_app ./data/data_10.csv './data/data_10(1).csv'
./app ./data/data_10.csv './data/data_10(1).csv'

echo "200000"
./cpu_app ./data/data_20.csv './data/data_20(1).csv'
./app ./data/data_20.csv './data/data_20(1).csv'

echo "300000"
./cpu_app ./data/data_30.csv './data/data_30(1).csv'
./app ./data/data_30.csv './data/data_30(1).csv'

echo "500000"
./cpu_app ./data/data_50.csv './data/data_50(1).csv'
./app ./data/data_50.csv './data/data_50(1).csv'

echo "700000"
./cpu_app ./data/data_70.csv './data/data_70(1).csv'
./app ./data/data_70.csv './data/data_70(1).csv'

echo "1000000"
./cpu_app ./data/data_100.csv './data/data_100(1).csv'
./app ./data/data_100.csv './data/data_100(1).csv'

make clean