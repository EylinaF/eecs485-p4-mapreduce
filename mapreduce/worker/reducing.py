import subprocess
import logging
import tempfile
import shutil
import heapq
import socket  # Added missing import
import json    # Added missing import
from pathlib import Path

LOGGER = logging.getLogger(__name__)

class WorkerTasks:
    def __init__(self, worker_host, worker_port, manager_host, manager_port):
        self.host = worker_host
        self.port = worker_port
        self.manager_host = manager_host
        self.manager_port = manager_port

    def send_task_finished(self, task_id, task_type="reduce"):
        """Notify Manager that a task has completed."""
        msg = {
            "message_type": "finished",  # Changed to match test expectation
            "task_id": task_id,
            "worker_host": self.host,
            "worker_port": self.port,
            # Removed task_type as test doesn't expect it
        }
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.settimeout(2)  # Added timeout
                sock.connect((self.manager_host, self.manager_port))
                sock.sendall(json.dumps(msg).encode("utf-8"))
                LOGGER.info("Sent reduce task %d completion", task_id)
        except Exception as e:
            LOGGER.error("Failed to send task completion: %s", e)

    def handle_reduce_task(self, task):
        """Execute a reduce task with temp directory."""
        task_id = task["task_id"]
        input_paths = task["input_paths"]
        executable = task["executable"]
        output_directory = Path(task["output_directory"])

        LOGGER.info("Starting reduce task %d with %d input files", task_id, len(input_paths))

        try:
            with tempfile.TemporaryDirectory(  # ADD THIS
                prefix=f"mapreduce-local-task{task_id:05d}-"
            ) as tmpdir:
                tmpdir_path = Path(tmpdir)
                output_file = tmpdir_path / f"part-{task_id:05d}"
            
                # Open all input files
                files = [open(path, "r") for path in input_paths]
            
                # Use merge-sort approach
                with open(output_file, "w") as outfile:
                    merged = heapq.merge(*files)
                
                    # Process through reducer
                    with subprocess.Popen(
                        [executable],
                        stdin=subprocess.PIPE,
                        stdout=outfile,
                        text=True,
                    ) as proc:
                        for line in merged:
                            proc.stdin.write(line)
                        proc.stdin.close()
                        proc.wait()

                # Move output from tempdir to final location
                final_output = output_directory / f"part-{task_id:05d}"
                shutil.move(str(output_file), str(final_output))

        except Exception as e:
            LOGGER.error("Reduce task %d failed: %s", task_id, e)
            raise
        finally:
            # Close all files
            for f in files:
                try:
                    if hasattr(f, 'close'):
                        f.close()
                except Exception as e:
                    LOGGER.warning("Error closing file: %s", e)

        self.send_task_finished(task_id)