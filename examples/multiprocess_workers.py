#!/usr/bin/env python3
"""
Example of using multiple worker processes to handle CPU-bound tasks.
"""

import logging
import math
import multiprocessing
import os
import random
import sys
import time
from datetime import datetime, timedelta

# Add the parent directory to the path so we can import the task_queue package
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from task_queue.producer import TaskProducer
from task_queue.worker import TaskWorker


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - PID:%(process)d - %(message)s'
)

# Define some CPU-intensive task handlers
def prime_factors(payload):
    """
    Compute the prime factors of a number (CPU-intensive task).
    """
    number = payload.get('number', 100000)
    start_time = time.time()
    
    logging.info(f"Computing prime factors of {number}")
    
    factors = []
    n = number
    
    # Check divisibility by 2
    while n % 2 == 0:
        factors.append(2)
        n //= 2
    
    # Check divisibility by odd numbers
    for i in range(3, int(math.sqrt(n)) + 1, 2):
        while n % i == 0:
            factors.append(i)
            n //= i
    
    # If n is a prime number greater than 2
    if n > 2:
        factors.append(n)
    
    elapsed = time.time() - start_time
    logging.info(f"Prime factors of {number}: {factors} (took {elapsed:.2f}s)")
    
    return {
        'number': number,
        'factors': factors,
        'elapsed': elapsed
    }


def fibonacci(payload):
    """
    Compute the n-th Fibonacci number recursively (very inefficient, CPU-intensive).
    """
    n = payload.get('n', 35)  # Default to a value that takes a few seconds
    start_time = time.time()
    
    logging.info(f"Computing Fibonacci({n})")
    
    def fib(k):
        if k <= 1:
            return k
        return fib(k-1) + fib(k-2)
    
    result = fib(n)
    
    elapsed = time.time() - start_time
    logging.info(f"Fibonacci({n}) = {result} (took {elapsed:.2f}s)")
    
    return {
        'n': n,
        'result': result,
        'elapsed': elapsed
    }


def monte_carlo_pi(payload):
    """
    Estimate Pi using a Monte Carlo method (CPU-intensive).
    """
    n = payload.get('iterations', 10000000)  # Default to 10 million points
    start_time = time.time()
    
    logging.info(f"Estimating Pi with {n} iterations")
    
    inside = 0
    for _ in range(n):
        x = random.random()
        y = random.random()
        if x*x + y*y <= 1:
            inside += 1
    
    pi_estimate = 4 * inside / n
    
    elapsed = time.time() - start_time
    logging.info(f"Pi estimate: {pi_estimate} (took {elapsed:.2f}s)")
    
    return {
        'iterations': n,
        'pi_estimate': pi_estimate,
        'elapsed': elapsed
    }


def start_worker_process(process_id, task_handlers):
    """Start a worker process with the given ID."""
    worker = TaskWorker(
        task_handlers=task_handlers,
        consumer_name=f"process-{process_id}",
        thread_count=1  # Use only one thread per process for CPU-bound tasks
    )
    
    logging.info(f"Worker process {process_id} starting")
    worker.start()
    
    try:
        # Keep the process running until interrupted
        while True:
            time.sleep(10)
    except KeyboardInterrupt:
        pass
    finally:
        logging.info(f"Worker process {process_id} stopping")
        worker.stop()


def add_cpu_tasks():
    """Add some CPU-intensive tasks to the queue."""
    producer = TaskProducer()
    
    # Add some prime factorization tasks
    for i in range(3):
        # Choose large numbers that will take a few seconds to factorize
        number = random.randint(100000000, 1000000000)
        task_id = producer.add_task(
            'prime_factors',
            {'number': number}
        )
        logging.info(f"Added prime_factors task for {number}, ID: {task_id}")
    
    # Add some Fibonacci calculation tasks
    for i in range(3):
        n = random.randint(35, 40)  # Values that take a few seconds
        task_id = producer.add_task(
            'fibonacci',
            {'n': n}
        )
        logging.info(f"Added fibonacci task for n={n}, ID: {task_id}")
    
    # Add some Pi calculation tasks
    for i in range(3):
        iterations = random.randint(5000000, 20000000)
        task_id = producer.add_task(
            'monte_carlo_pi',
            {'iterations': iterations}
        )
        logging.info(f"Added monte_carlo_pi task with {iterations} iterations, ID: {task_id}")


def main():
    """Run the multiprocessing example."""
    # Define task handlers
    task_handlers = {
        'prime_factors': prime_factors,
        'fibonacci': fibonacci,
        'monte_carlo_pi': monte_carlo_pi
    }
    
    # Add tasks to the queue
    add_cpu_tasks()
    
    # Determine the number of worker processes to start
    # For CPU-bound tasks, it's usually best to use one process per CPU core
    num_processes = multiprocessing.cpu_count()
    logging.info(f"Starting {num_processes} worker processes for {num_processes} CPU cores")
    
    # Start worker processes
    processes = []
    try:
        for i in range(num_processes):
            p = multiprocessing.Process(
                target=start_worker_process,
                args=(i, task_handlers)
            )
            p.daemon = True
            p.start()
            processes.append(p)
            
        logging.info(f"All {num_processes} worker processes started")
        
        # Keep the main process running
        try:
            while True:
                time.sleep(1)
                # Check if all tasks are done (in a real system, you'd use a proper monitoring method)
                alive = [p for p in processes if p.is_alive()]
                if not alive:
                    logging.info("All worker processes have terminated")
                    break
        except KeyboardInterrupt:
            logging.info("Received interrupt, shutting down...")
            
    finally:
        # Wait for all processes to finish
        for p in processes:
            try:
                p.join(timeout=2)
            except:
                pass
            
        logging.info("Example complete")


if __name__ == "__main__":
    main() 