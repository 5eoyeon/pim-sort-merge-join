import subprocess

commands = [
    ['make'],
    ['./cpu_app', './data/data_1.csv', './data/data_1(1).csv'],
    ['./app', './data/data_1.csv', './data/data_1(1).csv'],
    ['./cpu_app', './data/data_10.csv', './data/data_10(1).csv'],
    ['./app', './data/data_10.csv', './data/data_10(1).csv'],
    ['./cpu_app', './data/data_20.csv', './data/data_20(1).csv'],
    ['./app', './data/data_20.csv', './data/data_20(1).csv'],
    ['./cpu_app', './data/data_30.csv', './data/data_30(1).csv'],
    ['./app', './data/data_30.csv', './data/data_30(1).csv'],
    ['./cpu_app', './data/data_50.csv', './data/data_50(1).csv'],
    ['./app', './data/data_50.csv', './data/data_50(1).csv'],
    ['./cpu_app', './data/data_70.csv', './data/data_70(1).csv'],
    ['./app', './data/data_70.csv', './data/data_70(1).csv'],
    ['./cpu_app', './data/data_100.csv', './data/data_100(1).csv'],
    ['./app', './data/data_100.csv', './data/data_100(1).csv'],
    ['make', 'clean']
]

def run_command(command):
    try:
        result = subprocess.run(command, check=True, text=True, capture_output=True)
        print(f"\n{'=' * 40}") 
        print(f"Command: {' '.join(command)}")
        print("Output:")
        print(result.stdout.strip() if result.stdout.strip() else "No output") 
    except subprocess.CalledProcessError as e:
        print(f"\n{'=' * 40}")  
        print(f"Error Command: {' '.join(command)}")
        print("Error output:")
        print(e.stderr.strip() if e.stderr.strip() else "No error output")  

for command in commands:
    run_command(command)

print(f"\n{'=' * 40}")  