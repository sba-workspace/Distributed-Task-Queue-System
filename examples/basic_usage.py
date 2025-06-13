#!/usr/bin/env python3
"""
Basic example of using the distributed task queue.
"""

import json
import logging
import os
import time
from datetime import datetime, timedelta
import sys

# Add the parent directory to the path so we can import the task_queue package
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from task_queue.producer import TaskProducer
from task_queue.worker import TaskWorker
from task_queue.scheduler import TaskScheduler


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# Task handler functions
def echo_task(payload):
    """Echo task handler that simply returns the input payload."""
    logging.info(f"Processing echo task with payload: {payload}")
    return payload

def add_numbers(payload):
    """Add two numbers together."""
    a = payload.get('a', 0)
    b = payload.get('b', 0)
    result = a + b
    logging.info(f"Adding {a} + {b} = {result}")
    return result

def slow_task(payload):
    """A task that takes some time to complete."""
    seconds = payload.get('seconds', 5)
    logging.info(f"Starting slow task, will take {seconds} seconds")
    time.sleep(seconds)
    logging.info("Slow task completed")
    return {"slept_for": seconds}

def error_task(payload):
    """A task that always fails with an exception."""
    logging.info("Starting error task")
    raise ValueError("This task is designed to fail")


def run_producer():
    """Run a task producer to add some tasks to the queue."""
    producer = TaskProducer()
    
    # Add a simple echo task
    echo_id = producer.add_task(
        'echo',
        {"message": "Hello, World!"}
    )
    logging.info(f"Added echo task with ID: {echo_id}")
    
    # Add some math tasks
    for i in range(5):
        add_id = producer.add_task(
            'add',
            {"a": i, "b": i * 2}
        )
        logging.info(f"Added add task with ID: {add_id}")
    
    # Add a slow task
    slow_id = producer.add_task(
        'slow',
        {"seconds": 3}
    )
    logging.info(f"Added slow task with ID: {slow_id}")
    
    # Add a task that will fail
    error_id = producer.add_task(
        'error',
        {"message": "This will cause an error"}
    )
    logging.info(f"Added error task with ID: {error_id}")
    
    # Schedule a task for the future
    future_time = datetime.now() + timedelta(seconds=10)
    scheduled_id = producer.schedule_task(
        'echo',
        {"message": "This is a delayed task", "scheduled": True},
        execute_at=future_time.timestamp()
    )
    logging.info(f"Scheduled echo task with ID: {scheduled_id} for {future_time}")
    
    return {
        'echo_id': echo_id,
        'slow_id': slow_id,
        'error_id': error_id,
        'scheduled_id': scheduled_id
    }


def run_worker():
    """Run a worker to process tasks from the queue."""
    # Create a task handler map
    task_handlers = {
        'echo': echo_task,
        'add': add_numbers,
        'slow': slow_task,
        'error': error_task
    }
    
    # Create and start the worker
    worker = TaskWorker(
        task_handlers=task_handlers,
        thread_count=3  # Use 3 worker threads
    )
    
    logging.info("Starting worker...")
    worker.start()
    
    return worker


def run_scheduler():
    """Run a scheduler to process delayed tasks."""
    scheduler = TaskScheduler()
    
    logging.info("Starting scheduler...")
    scheduler.start()
    
    return scheduler


def main():
    """Run the example."""
    logging.info("Starting basic task queue example")
    
    # Run the scheduler (for delayed tasks)
    scheduler = run_scheduler()
    
    # Run the worker
    worker = run_worker()
    
    # Run the producer to add tasks
    task_ids = run_producer()
    
    try:
        # Keep the main thread alive so the workers can process
        logging.info("Tasks have been added to the queue. Press Ctrl+C to exit.")
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logging.info("Stopping worker and scheduler...")
        worker.stop()
        scheduler.stop()
        
        logging.info("Example complete.")


if __name__ == "__main__":
    main() 