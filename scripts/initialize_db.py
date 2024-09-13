import duckdb
import os
import subprocess

def set_and_get_data_path():
    # First, try to get the DATA_PATH from the environment
    DATA_PATH = os.getenv("DATA_PATH")

    # If not set, prompt the user to enter it
    if not DATA_PATH:
        shell_config = os.path.expanduser('~/.bashrc')  # or ~/.zshrc for Zsh

        data_path = input("DATA_PATH is not set. Please enter the path where your data is stored: ")
        
        # Write to the shell configuration file
        with open(shell_config, 'a') as file:
            file.write(f'\nexport DATA_PATH="{data_path}"\n')
        
        print(f"DATA_PATH has been added to {shell_config}")
        print("Please run 'source ~/.bashrc' (or the appropriate config file) in your terminal,")
        print("or restart your terminal for the changes to take effect.")
        print("Then, run this script again.")
        
        # Exit the script
        exit(0)
    
    return DATA_PATH

# Get the DATA_PATH
DATA_PATH = set_and_get_data_path()

print(f"DATA_PATH is set to: {DATA_PATH}")

data_dir = os.path.expanduser(DATA_PATH)
db_file = os.path.join(data_dir, 'munincipal_data.db')

# Check if the directory exists, if not, create it
if not os.path.exists(data_dir):
    os.makedirs(data_dir)
    print(f"Directory {data_dir} created successfully!")
else:
    print(f"Directory {data_dir} already exists.")

# Connect to DuckDB database (this will create the file if it doesn't exist)
con = duckdb.connect(db_file)

# Create a staging schema
con.execute('CREATE SCHEMA IF NOT EXISTS raw;')

print("Raw schema created successfully!")

# Close the connection
con.close()