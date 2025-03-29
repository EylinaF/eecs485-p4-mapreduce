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
        """
        Run a MapReduce job if the queue is not empty.
        Ensures jobs are executed sequentially.
        """
        if self.current_job:
            LOGGER.info("A job is already running. Waiting for completion.")
            return  # Do not run multiple jobs at once

        if self.job_queue.empty():
            LOGGER.info("No jobs in the queue.")
            return

        # Get the next job
        job = self.job_queue.get()
        self.current_job = job
        job_id = job["job_id"]
        output_dir = job["output_directory"]
        intermediate_dir = os.path.join(self.shared_dir, f"job-{job_id:05d}")

        # Step 1: Delete existing output directory if it exists
        if os.path.exists(output_dir):
            shutil.rmtree(output_dir)
        os.makedirs(output_dir)

        # Step 2: Create intermediate directory
        os.makedirs(intermediate_dir, exist_ok=True)

        LOGGER.info(f"Job {job_id} started: Intermediate dir -> {intermediate_dir}")

        # TODO: Implement logic to assign work to workers.

        # Mark job as complete (for now, just log it)
        LOGGER.info(f"Job {job_id} completed.")
        self.current_job = None