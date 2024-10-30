import subprocess

commands = [
    ['make'],
    ['./cpu_app', './data/data1.csv', './data/data2.csv', '0', '5000','0', '5000', '0', '0'],
    ['./app', './data/data1.csv', './data/data2.csv', '0', '5000','0', '5000', '0', '0'],
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