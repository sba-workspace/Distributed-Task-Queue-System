import json
import logging
import os
import socket
import time
from concurrent.futures import ThreadPoolExecutor
import threading
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

import redis


class TaskWorker:
    """
    Worker for consuming and processing tasks from the Redis Stream queue.
    """
    
    def __init__(
        self,
        task_handlers: Dict[str, Callable],
        redis_host: str = 'localhost',
        redis_port: int = 6379,
        redis_db: int = 0,
        stream_name: str = 'tasks',
        consumer_group: str = 'workers',
        consumer_name: Optional[str] = None,
        block_ms: int = 5000,
        idle_timeout_ms: int = 60000,
        thread_count: int = 1
    ):
        """
        Initialize the task worker.
        
        Args:
            task_handlers: Dictionary mapping task types to handler functions
            redis_host: Redis server hostname
            redis_port: Redis server port
            redis_db: Redis database number
            stream_name: Name of the Redis stream to use as the task queue
            consumer_group: Name of the consumer group
            consumer_name: Unique name for this consumer (defaults to hostname:pid)
            block_ms: How long to block waiting for new tasks, in milliseconds
            idle_timeout_ms: Time in ms after which a task is considered stale
            thread_count: Number of worker threads to use for processing tasks
        """
        self.task_handlers = task_handlers
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.redis_db = redis_db
        self.stream_name = stream_name
        self.consumer_group = consumer_group
        
        # Generate default consumer name if not provided
        if consumer_name is None:
            hostname = socket.gethostname()
            pid = os.getpid()
            self.consumer_name = f"{hostname}:{pid}"
        else:
            self.consumer_name = consumer_name
            
        self.block_ms = block_ms
        self.idle_timeout_ms = idle_timeout_ms
        self.thread_count = thread_count
        self.running = False
        self.client = self._create_redis_client()
        self.logger = logging.getLogger("TaskWorker")
        
        # Ensure the consumer group exists
        self._setup_consumer_group()
        
    def _create_redis_client(self) -> redis.Redis:
        """Create and return a Redis client."""
        return redis.Redis(
            host=self.redis_host,
            port=self.redis_port,
            db=self.redis_db,
            decode_responses=True
        )
        
    def _setup_consumer_group(self) -> None:
        """Set up the Redis Stream consumer group."""
        try:
            # Create the consumer group if it doesn't exist
            # Use $ as the start ID to only process new messages
            self.client.xgroup_create(
                self.stream_name, 
                self.consumer_group,
                id='$',  # Start with new messages only
                mkstream=True  # Create stream if it doesn't exist
            )
        except redis.exceptions.ResponseError as e:
            # Ignore error if the group already exists
            if 'BUSYGROUP' not in str(e):
                raise
                
    def start(self) -> None:
        """Start the worker and its processing threads."""
        if self.running:
            return
            
        self.running = True
        self.logger.info(f"Starting worker {self.consumer_name} with {self.thread_count} threads")
        
        # Create a thread pool for processing tasks
        self.executor = ThreadPoolExecutor(max_workers=self.thread_count)
        
        # Start a thread for reclaiming stale tasks
        self.reclaim_thread = threading.Thread(
            target=self._reclaim_stale_tasks, 
            daemon=True
        )
        self.reclaim_thread.start()
        
        # Start worker threads
        self.worker_threads = []
        for i in range(self.thread_count):
            thread = threading.Thread(
                target=self._worker_loop,
                args=(i,),
                daemon=True
            )
            thread.start()
            self.worker_threads.append(thread)
            
    def stop(self) -> None:
        """Stop the worker and all its threads."""
        if not self.running:
            return
            
        self.logger.info(f"Stopping worker {self.consumer_name}")
        self.running = False
        
        # Wait for threads to finish
        for thread in self.worker_threads:
            thread.join(timeout=5.0)
        
        # Shutdown the executor
        self.executor.shutdown(wait=True)
        
        # Wait for the reclaim thread
        self.reclaim_thread.join(timeout=5.0)
        
    def _worker_loop(self, thread_id: int) -> None:
        """Main worker loop for consuming and processing tasks."""
        # Each thread gets its own Redis connection from the pool
        redis_client = self._create_redis_client()
        
        self.logger.info(f"Worker thread {thread_id} started")
        
        while self.running:
            try:
                # Read new messages from the stream as part of the consumer group
                # Use > as ID to get only new (never delivered) messages
                entries = redis_client.xreadgroup(
                    groupname=self.consumer_group,
                    consumername=f"{self.consumer_name}:{thread_id}",
                    streams={self.stream_name: '>'},
                    count=1,
                    block=self.block_ms
                )
                
                # Process any received entries
                if entries:
                    for stream_name, messages in entries:
                        for message_id, data in messages:
                            self._process_task(redis_client, message_id, data)
            
            except Exception as e:
                self.logger.error(f"Error in worker loop: {e}")
                time.sleep(1)  # Avoid tight error loops
                
        self.logger.info(f"Worker thread {thread_id} stopped")
        
    def _process_task(
        self, 
        redis_client: redis.Redis, 
        message_id: str, 
        data: Dict[str, str]
    ) -> None:
        """
        Process a single task from the stream.
        
        Args:
            redis_client: Redis client to use
            message_id: ID of the message in the stream
            data: Task data from the stream
        """
        task_id = data.get('task_id')
        task_type = data.get('task_type')
        
        if not task_id or not task_type:
            self.logger.warning(f"Invalid task format: {data}")
            # Acknowledge invalid tasks to remove them from the pending list
            redis_client.xack(self.stream_name, self.consumer_group, message_id)
            return
            
        self.logger.info(f"Processing task {task_id} of type {task_type}")
        
        # Update task status to running
        redis_client.hset(f"task_status:{task_id}", mapping={
            'state': 'running',
            'started_at': time.time(),
            'worker': self.consumer_name
        })
        
        try:
            # Find the handler for this task type
            handler = self.task_handlers.get(task_type)
            if not handler:
                raise ValueError(f"No handler registered for task type: {task_type}")
                
            # Parse the payload
            payload = json.loads(data.get('payload', '{}'))
            
            # Execute the task handler
            result = handler(payload)
            
            # Store the result
            redis_client.hset(f"task_result:{task_id}", mapping={
                'success': True,
                'result': json.dumps(result) if result is not None else '',
                'completed_at': time.time()
            })
            
            # Update the status to completed
            redis_client.hset(f"task_status:{task_id}", mapping={
                'state': 'completed',
                'completed_at': time.time()
            })
            
            # Acknowledge the message to remove it from the pending list
            redis_client.xack(self.stream_name, self.consumer_group, message_id)
            
            self.logger.info(f"Task {task_id} completed successfully")
            
        except Exception as e:
            self.logger.error(f"Error processing task {task_id}: {e}")
            
            # Increment the attempts counter
            attempts = int(data.get('attempts', 0)) + 1
            
            # If we've reached max retries, mark as failed and acknowledge
            max_retries = 3  # Could be configurable
            if attempts >= max_retries:
                # Store the error
                redis_client.hset(f"task_result:{task_id}", mapping={
                    'success': False,
                    'error': str(e),
                    'completed_at': time.time()
                })
                
                # Update status to failed
                redis_client.hset(f"task_status:{task_id}", mapping={
                    'state': 'failed',
                    'error': str(e),
                    'completed_at': time.time()
                })
                
                # Acknowledge the message
                redis_client.xack(self.stream_name, self.consumer_group, message_id)
                
                self.logger.warning(f"Task {task_id} failed after {attempts} attempts")
            else:
                # Don't acknowledge - let the task be reclaimed later
                # Or we could re-add it to the queue with incremented attempts
                self.logger.info(f"Task {task_id} will be retried (attempt {attempts})")
                
                # Update the data with the new attempt count
                data['attempts'] = str(attempts)
                
                # Re-add the task to the queue
                redis_client.xadd(self.stream_name, data)
                
                # Acknowledge the original message
                redis_client.xack(self.stream_name, self.consumer_group, message_id)
    
    def _reclaim_stale_tasks(self) -> None:
        """
        Background thread to reclaim stale tasks from other consumers.
        This handles tasks from consumers that have died or are hanging.
        """
        redis_client = self._create_redis_client()
        
        while self.running:
            try:
                # Use XAUTOCLAIM to find and claim idle messages
                result = redis_client.xautoclaim(
                    self.stream_name,
                    self.consumer_group,
                    self.consumer_name,
                    min_idle_time=self.idle_timeout_ms,
                    count=10
                )
                
                # Process any claimed messages
                claimed_messages = result[1]  # [0] is the next cursor ID
                for message_id, data in claimed_messages:
                    self.logger.info(f"Reclaimed stale task: {message_id}")
                    self._process_task(redis_client, message_id, data)
                    
                # Sleep before next reclaim check
                time.sleep(30)
                    
            except Exception as e:
                self.logger.error(f"Error in reclaim loop: {e}")
                time.sleep(10)  # Avoid tight error loops 