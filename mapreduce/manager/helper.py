"""This is helper for Manager."""
import queue
import os
import shutil
import logging

LOGGER = logging.getLogger(__name__)

class JobManager:
    """Handles job queueing and ID assignment for the MapReduce Manager."""

    def __init__(self, shared_dir):
        """Initialize job queue and job counter."""
        self.job_queue = queue.Queue()
        self.job_id_counter = 0
        self.shared_dir = shared_dir  # Directory for intermediate files
        self.current_job = None  # Keep track of running job
        self.current_job_tasks = []

    def new_job_request(self, job_data):
        """
        Handle a new job request by assigning a job ID and adding it to the queue.
        
        Args:
            job_data (dict): A dictionary containing job details.

        Returns:
            int: The assigned job ID.
        """
        job_id = self.job_id_counter
        self.job_id_counter += 1

        job = {
            "job_id": job_id,
            "input_directory": job_data["input_directory"],
            "output_directory": job_data["output_directory"],
            "mapper_executable": job_data["mapper_executable"],
            "reducer_executable": job_data["reducer_executable"],
            "num_mappers": job_data["num_mappers"],
            "num_reducers": job_data["num_reducers"],
        }

        self.job_queue.put(job)
        LOGGER.info(f"New job {job_id} added to the queue")

        return job_id

    
    def run_job(self):
        if self.current_job:
            LOGGER.info("A job is already running. Waiting for completion.")
            return

        if self.job_queue.empty():
            LOGGER.info("No jobs in the queue.")
            return

        job = self.job_queue.get()
        self.current_job = job
        job_id = job["job_id"]
        intermediate_dir = os.path.join(self.shared_dir, f"job-{job_id:05d}")

        # Setup directories
        if os.path.exists(job["output_directory"]):
            shutil.rmtree(job["output_directory"])
        os.makedirs(job["output_directory"], exist_ok=True)
        os.makedirs(intermediate_dir, exist_ok=True)

        # Generate map tasks
        self.current_job_tasks = self._create_map_tasks(job, intermediate_dir)
        LOGGER.info(f"Job {job_id} created {len(self.current_job_tasks)} map tasks.")
        return

    def _create_map_tasks(self, job, intermediate_dir):
        input_dir = job["input_directory"]
        num_mappers = job["num_mappers"]
        num_reducers = job["num_reducers"]

        # List and sort input files lexicographically
        input_files = sorted([
            os.path.join(input_dir, f)
            for f in os.listdir(input_dir)
            if os.path.isfile(os.path.join(input_dir, f))
        ], key=lambda x: os.path.basename(x))

        # Partition files using round-robin
        partitions = [[] for _ in range(num_mappers)]
        for idx, file_path in enumerate(input_files):
            partitions[idx % num_mappers].append(file_path)

        # Create task dictionaries
        tasks = []
        for task_id, input_paths in enumerate(partitions):
            if not input_paths:
                continue  # Skip empty partitions
            tasks.append({
                "task_id": task_id,
                "input_paths": input_paths,
                "executable": job["mapper_executable"],
                "output_directory": os.path.join(intermediate_dir),
                "num_partitions": num_reducers,
            })
        return tasks

    def create_reduce_tasks(self, intermediate_dir):
        """Generate reduce tasks from map output partitions."""
        job = self.current_job
        num_reducers = job["num_reducers"]
        reduce_tasks = []

        # Group map output files by partition
        part_files = defaultdict(list)
        for filename in os.listdir(intermediate_dir):
            if "-part" in filename:
                base, partition = filename.split("-part")
                partition_num = int(partition)
                part_files[partition_num].append(os.path.join(intermediate_dir, filename))

        # Create one reduce task per partition up to num_reducers
        for partition in range(num_reducers):
            input_paths = part_files.get(partition, [])
            if not input_paths:
                continue
            reduce_tasks.append({
                "task_id": partition,  # task_id = partition number
                "input_paths": input_paths,
                "executable": job["reducer_executable"],
                "output_directory": job["output_directory"],  # Final output dir
            })
        return reduce_tasks


    