import subprocess
import tempfile
import shutil
import heapq
import logging
import socket
import json
from pathlib import Path

LOGGER = logging.getLogger(__name__)

def handle_reduce_task(worker, task):
    task_id = task["task_id"]
    executable = task["executable"]
    input_paths = [Path(p) for p in task["input_paths"]]
    output_directory = Path(task["output_directory"])
    output_filename = f"part-{task_id:05d}"

    LOGGER.info("Started reduce task %d", task_id)

    with tempfile.TemporaryDirectory(prefix=f"mapreduce-local-task{task_id:05d}-") as tmpdir:
        tmpdir_path = Path(tmpdir)
        output_path = tmpdir_path / output_filename

        file_objects = [open(p, "r") for p in input_paths]

        try:
            merged_iter = heapq.merge(*file_objects)

            with open(output_path, "w") as outfile:
                with subprocess.Popen(
                    [executable],
                    text=True,
                    stdin=subprocess.PIPE,
                    stdout=outfile,
                ) as reduce_proc:
                    for line in merged_iter:
                        if reduce_proc.stdin:
                            reduce_proc.stdin.write(line)
                    reduce_proc.stdin.close()
                    reduce_proc.wait()

            output_directory.mkdir(parents=True, exist_ok=True)
            shutil.move(str(output_path), str(output_directory / output_filename))

        finally:
            for f in file_objects:
                f.close()

    LOGGER.info("Completed reduce task %d", task_id)
    send_task_finished(worker, task_id)

def send_task_finished(worker, task_id):
    msg = {
        "message_type": "finished",
        "task_id": task_id,
        "worker_host": worker.host,
        "worker_port": worker.port,
    }

    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect((worker.manager_host, worker.manager_port))
            sock.sendall(json.dumps(msg).encode("utf-8"))
            LOGGER.debug("Sent finished message for task %d", task_id)
    except Exception as e:
        LOGGER.error("Failed to send finished message: %s", e)