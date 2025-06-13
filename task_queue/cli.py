#!/usr/bin/env python3
"""
Command-line interface for starting the task queue components.
"""

import argparse
import logging
import os
import signal
import sys
import time

from task_queue.worker import TaskWorker
from task_queue.scheduler import TaskScheduler
from task_queue.cli_monitor import TaskQueueCLI


def setup_logging(verbose=False):
    """Set up logging configuration."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )


def run_worker(args):
    """Run a worker process."""
    # This is just a placeholder - in a real system, we would
    # dynamically load task handlers based on configuration
    from examples.basic_usage import echo_task, add_numbers, slow_task, error_task
    
    task_handlers = {
        'echo': echo_task,
        'add': add_numbers,
        'slow': slow_task,
        'error': error_task
    }
    
    worker = TaskWorker(
        redis_host=args.host,
        redis_port=args.port,
        redis_db=args.db,
        stream_name=args.stream,
        consumer_group=args.group,
        consumer_name=args.consumer_name,
        task_handlers=task_handlers,
        thread_count=args.threads
    )
    
    logging.info(f"Starting worker with {args.threads} threads")
    worker.start()
    
    def signal_handler(sig, frame):
        logging.info("Shutting down worker...")
        worker.stop()
        sys.exit(0)
        
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logging.info("Shutting down worker...")
        worker.stop()


def run_scheduler(args):
    """Run a scheduler process."""
    scheduler = TaskScheduler(
        redis_host=args.host,
        redis_port=args.port,
        redis_db=args.db,
        stream_name=args.stream,
        delayed_key=args.delayed_key,
        check_interval=args.interval
    )
    
    logging.info("Starting scheduler")
    scheduler.start()
    
    def signal_handler(sig, frame):
        logging.info("Shutting down scheduler...")
        scheduler.stop()
        sys.exit(0)
        
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logging.info("Shutting down scheduler...")
        scheduler.stop()


def run_monitor(args):
    """Run the CLI monitor."""
    cli = TaskQueueCLI(
        redis_host=args.host,
        redis_port=args.port,
        redis_db=args.db,
        stream_name=args.stream,
        consumer_group=args.group,
        delayed_key=args.delayed_key,
        refresh_interval=args.refresh
    )
    
    if args.command == 'dashboard':
        cli.dashboard()
    elif args.command == 'stats':
        cli.show_stats()
    elif args.command == 'consumers':
        cli.show_consumers()
    elif args.command == 'pending':
        cli.show_pending_tasks()
    elif args.command == 'recent':
        cli.show_recent_tasks()
    elif args.command == 'delayed':
        cli.show_delayed_tasks()
    elif args.command == 'task' and args.task_id:
        cli.show_task_details(args.task_id)
    else:
        # Default to stats
        cli.show_stats()


def main():
    """Main entry point for the CLI."""
    parser = argparse.ArgumentParser(description="Task Queue CLI")
    
    # Common options
    parser.add_argument('--host', default='localhost', help="Redis host")
    parser.add_argument('--port', type=int, default=6379, help="Redis port")
    parser.add_argument('--db', type=int, default=0, help="Redis database number")
    parser.add_argument('--stream', default='tasks', help="Redis stream name")
    parser.add_argument('--group', default='workers', help="Consumer group name")
    parser.add_argument('--delayed-key', default='delayed:tasks',
                        help="Redis key for delayed tasks")
    parser.add_argument('-v', '--verbose', action='store_true', help="Enable verbose logging")
    
    subparsers = parser.add_subparsers(dest='component', help="Component to run")
    
    # Worker options
    worker_parser = subparsers.add_parser('worker', help="Start a worker")
    worker_parser.add_argument('--threads', type=int, default=4,
                              help="Number of worker threads")
    worker_parser.add_argument('--consumer-name', help="Unique name for this consumer")
    
    # Scheduler options
    scheduler_parser = subparsers.add_parser('scheduler', help="Start a task scheduler")
    scheduler_parser.add_argument('--interval', type=float, default=1.0,
                                 help="Check interval in seconds")
    
    # Monitor options
    monitor_parser = subparsers.add_parser('monitor', help="Start the monitor")
    monitor_parser.add_argument('--refresh', type=float, default=2.0,
                               help="Refresh interval in seconds")
    monitor_parser.add_argument('command', nargs='?', choices=[
        'dashboard', 'stats', 'consumers', 'pending', 'recent', 'delayed', 'task'
    ], default='stats', help="Monitor command to run")
    monitor_parser.add_argument('task_id', nargs='?', help="Task ID for task details")
    
    args = parser.parse_args()
    
    setup_logging(args.verbose)
    
    if args.component == 'worker':
        run_worker(args)
    elif args.component == 'scheduler':
        run_scheduler(args)
    elif args.component == 'monitor':
        run_monitor(args)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main() 