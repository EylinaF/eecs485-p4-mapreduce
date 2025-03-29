import subprocess
import tempfile
import hashlib
import shutil
import logging
import socket
import json
from pathlib import Path

LOGGER = logging.getLogger(__name__)

class WorkerTasks:
    def __init__(self, worker_host, worker_port, manager_host, manager_port):
        self.host = worker_host
        self.port = worker_port
        self.manager_host = manager_host
        self.manager_port = manager_port

    def send_task_finished(self, task_id, task_type="map"):
        """Send the EXACT message format the test expects."""
        msg = {
            "message_type": "finished",  # Must be "finished" not "task_complete"
            "task_id": task_id,
            "worker_host": self.host,
            "worker_port": self.port,
            # Note: Test doesn't expect "task_type" field
        }
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.settimeout(2)
                sock.connect((self.manager_host, self.manager_port))
                sock.sendall(json.dumps(msg).encode("utf-8"))
                LOGGER.info("Sent task %d completion", task_id)
        except Exception as e:
            LOGGER.error("Failed to send completion: %s", e)

    def handle_map_task(self, task):
        """Execute a map task with proper partitioning and sorting."""
        task_id = task["task_id"]
        input_paths = task["input_paths"]
        executable = task["executable"]
        output_directory = Path(task["output_directory"])
        num_partitions = task["num_partitions"]

        LOGGER.info("Starting map task %d with %d partitions", task_id, num_partitions)

        try:
            with tempfile.TemporaryDirectory(
                prefix=f"mapreduce-local-task{task_id:05d}-"
            ) as tmpdir:
                tmpdir_path = Path(tmpdir)
                partition_files = {}

                # Process each input file
                for input_path in input_paths:
                    with open(input_path, "r") as infile:
                        with subprocess.Popen(
                            [executable],
                            stdin=infile,
                            stdout=subprocess.PIPE,
                            text=True,
                        ) as proc:
                            for line in proc.stdout:
                                if not line.strip():
                                    continue
                                try:
                                    key = line.split("\t", 1)[0]
                                    partition = int(
                                        hashlib.md5(key.encode("utf-8")).hexdigest(), 16
                                    ) % num_partitions
                                    if partition not in partition_files:
                                        partition_files[partition] = open(
                                            tmpdir_path / f"maptask{task_id:05d}-part{partition:05d}",
                                            "w"
                                        )
                                    partition_files[partition].write(line)
                                except Exception as e:
                                    LOGGER.warning("Error processing line: %s", e)

                # Close all partition files
                for f in partition_files.values():
                    f.close()

                # Sort and move partitions
                output_directory.mkdir(parents=True, exist_ok=True)
                for partition in partition_files:
                    filename = tmpdir_path / f"maptask{task_id:05d}-part{partition:05d}"
                    subprocess.run(["sort", "-o", filename, filename], check=True)
                    shutil.move(str(filename), str(output_directory / filename.name))

        except Exception as e:
            LOGGER.error("Map task %d failed: %s", task_id, e)
            raise
        finally:
            # Ensure all files are closed
            for f in partition_files.values():
                if not f.closed:
                    f.close()

        self.send_task_finished(task_id, "map")