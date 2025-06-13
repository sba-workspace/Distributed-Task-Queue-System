#!/usr/bin/env python3
"""
Example demonstrating the CLI monitor with generated tasks to visualize task states.
"""

import logging
import os
import random
import sys
import time
import threading
from datetime import datetime, timedelta

# Add the parent directory to the path so we can import the task_queue package
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from task_queue.producer import TaskProducer
from task_queue.worker import TaskWorker
from task_queue.scheduler import TaskScheduler
from task_queue.cli_monitor import TaskQueueCLI


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("monitor_demo")


# Define some task handlers
def fast_task(payload):
    """A fast task that completes quickly."""
    task_id = payload.get('task_id', 'unknown')
    logger.info(f"Processing fast task {task_id}")
    time.sleep(0.2)  # Small delay to simulate some work
    return {"status": "completed", "task_id": task_id}


def medium_task(payload):
    """A medium-length task."""
    task_id = payload.get('task_id', 'unknown')
    logger.info(f"Processing medium task {task_id}")
    time.sleep(1.0)  # 1 second delay
    return {"status": "completed", "task_id": task_id}


def slow_task(payload):
    """A slow task that takes longer to complete."""
    task_id = payload.get('task_id', 'unknown')
    duration = payload.get('duration', 3)
    logger.info(f"Processing slow task {task_id}, will take {duration}s")
    
    for i in range(duration):
        time.sleep(1.0)
        logger.info(f"Slow task {task_id}: {i+1}/{duration}s completed")
    
    return {"status": "completed", "task_id": task_id, "duration": duration}


def fail_task(payload):
    """A task that always fails."""
    task_id = payload.get('task_id', 'unknown')
    logger.info(f"Processing failing task {task_id}")
    time.sleep(0.5)  # Do some work before failing
    raise ValueError(f"Task {task_id} failed as expected")


class ContinuousProducer(threading.Thread):
    """Thread to continuously add tasks at random intervals."""
    
    def __init__(self, task_types, stop_event):
        super().__init__(daemon=True)
        self.task_types = task_types
        self.stop_event = stop_event
        self.producer = TaskProducer()
    
    def run(self):
        """Add tasks at random intervals."""
        task_count = 0
        
        while not self.stop_event.is_set():
            task_count += 1
            
            # Pick a random task type
            task_type = random.choice(self.task_types)
            
            # Create task payload
            payload = {"task_id": f"continuous-{task_count}", "timestamp": time.time()}
            
            # Add some specific parameters for different task types
            if task_type == 'slow':
                payload['duration'] = random.randint(3, 8)
                
            # Decide whether to schedule this task or run immediately
            scheduled = random.random() < 0.3  # 30% chance of scheduling
            
            if scheduled:
                # Schedule for 5-20 seconds in the future
                delay = random.uniform(5, 20)
                future_time = datetime.now() + timedelta(seconds=delay)
                task_id = self.producer.schedule_task(
                    task_type,
                    payload,
                    execute_at=future_time.timestamp()
                )
                logger.info(f"Scheduled {task_type} task {task_id} for {delay:.1f}s from now")
            else:
                # Add for immediate execution
                task_id = self.producer.add_task(task_type, payload)
                logger.info(f"Added {task_type} task {task_id}")
                
            # Sleep for a random interval before adding the next task
            sleep_time = random.uniform(1, 3)
            
            # Use small sleep intervals and check stop_event to allow clean shutdown
            for _ in range(int(sleep_time * 10)):
                if self.stop_event.is_set():
                    break
                time.sleep(0.1)


def setup_system():
    """Initialize the task queue system with workers and a scheduler."""
    # Create task handlers
    task_handlers = {
        'fast': fast_task,
        'medium': medium_task,
        'slow': slow_task,
        'fail': fail_task,
    }
    
    # Start a worker with multiple threads
    worker = TaskWorker(
        task_handlers=task_handlers,
        thread_count=4,  # Use 4 worker threads
    )
    worker.start()
    
    # Start a scheduler for delayed tasks
    scheduler = TaskScheduler()
    scheduler.start()
    
    return worker, scheduler


def main():
    """Run the monitor demo."""
    logger.info("Starting monitor demo")
    
    # Set up the worker and scheduler
    worker, scheduler = setup_system()
    
    # Create a stop event for the continuous producer
    stop_event = threading.Event()
    
    # Start the continuous producer
    producer = ContinuousProducer(
        task_types=['fast', 'medium', 'slow', 'fail'],
        stop_event=stop_event
    )
    producer.start()
    
    # Add some initial tasks to get started
    initial_producer = TaskProducer()
    for i in range(5):
        initial_producer.add_task('fast', {"task_id": f"initial-fast-{i}"})
        initial_producer.add_task('medium', {"task_id": f"initial-medium-{i}"})
    
    initial_producer.add_task('slow', {"task_id": "initial-slow", "duration": 10})
    initial_producer.add_task('fail', {"task_id": "initial-fail"})
    
    # Schedule some future tasks
    for i in range(3):
        delay = random.uniform(3, 15)
        future_time = datetime.now() + timedelta(seconds=delay)
        initial_producer.schedule_task(
            'medium',
            {"task_id": f"initial-scheduled-{i}"},
            execute_at=future_time.timestamp()
        )
    
    logger.info("Initial tasks added, starting CLI monitor in dashboard mode...")
    logger.info("Press Ctrl+C to exit")
    
    # Start the monitor in dashboard mode
    try:
        cli = TaskQueueCLI(refresh_interval=1.0)
        cli.dashboard()
    except KeyboardInterrupt:
        pass
    finally:
        # Clean up
        logger.info("Shutting down...")
        stop_event.set()  # Signal the producer to stop
        worker.stop()
        scheduler.stop()
        
        # Wait for the producer to finish
        producer.join(2.0)
        
        logger.info("Monitor demo complete.")


if __name__ == "__main__":
    main() 