import os
import json
import time
import glob
import subprocess
import sys
from datetime import datetime

def main():
    # Replicate environment logging
    with open("env.log", "w") as f:
        json.dump(dict(os.environ), f, indent=4)

    # Replicate output redirection
    sys.stdout = open("stdout.log", "a")
    sys.stderr = open("stderr.log", "w") # Open in 'w' first to clear/create, then 'a' if needed or handled by system
    sys.stderr.close() # Immediately close it. Subsequent writes will be appends
    sys.stderr = open("stderr.log", "a")


    n_executions = 0
    n_sleeps = 0

    print(f"[{datetime.now()}]  -  Starting SLURM waiting loop ...")
    sys.stdout.flush()

    while True:
        # Check for exit condition
        if glob.glob("*.gitlab_ci_exit"):
            print(f"[{datetime.now()}]  -  Exiting SLURM waiting loop ...")
            sys.exit(0)

        # Search for script to execute
        scripts_to_run = glob.glob("*.gitlab_ci_step_script")
        if scripts_to_run:
            n_executions += 1
            script_path = scripts_to_run[0]

            time.sleep(1) # Short sleep like in bash script
            print(f"[{datetime.now()}]  -  Starting execution of {script_path} ...")

            log_script_path = f"{script_path}.log"
            try:
                with open(log_script_path, "w") as log_file:
                    process = subprocess.run(["bash", script_path], capture_output=True, text=True, check=True)
                    log_file.write(process.stdout)
                    log_file.write(process.stderr)
            except subprocess.CalledProcessError as e:
                print(f"[{datetime.now()}]  -  Error executing {script_path}")
                # Ensure stderr from the script is written to its log
                if e.stderr:
                     with open(log_script_path, "a") as log_file: # append in case stdout was already written
                        log_file.write(e.stderr)
                sys.exit(1)

            executed_script_path = f"{script_path}.executed"
            # Create the executed script file and copy content
            with open(script_path, "r") as original_script, open(executed_script_path, "w") as executed_file:
                executed_file.write(original_script.read())

            os.remove(script_path)

            # Flush NFS caches
            # os.syncfs is Linux specific, using subprocess for broader compatibility for sync
            subprocess.run(["sync", "-f", executed_script_path], check=False) # check=False as sync might not support -f on all systems
            subprocess.run(["sync", "-f"], check=False)
            subprocess.run(["sync"], check=False)

            # Touch current directory
            cwd = os.getcwd()
            os.utime(cwd, None) # Updates access and modification times to current time

            print(f"[{datetime.now()}]  -  Finished execution of {script_path}")

        time.sleep(1)
        n_sleeps += 1

        # Timeout condition
        if n_executions == 0 and n_sleeps > 600:
            print(f"[{datetime.now()}]  -  No executions in the last 10 minutes, exiting with error")
            sys.exit(1)


if __name__ == "__main__":
    main()
