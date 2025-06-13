import json
import logging
import threading
import time
from typing import Optional

import redis


class TaskScheduler:
    """
    Scheduler for handling delayed tasks stored in a Redis sorted set.
    Periodically checks for tasks that are due and moves them to the main task stream.
    """
    
    def __init__(
        self,
        redis_host: str = 'localhost',
        redis_port: int = 6379,
        redis_db: int = 0,
        stream_name: str = 'tasks',
        delayed_key: str = 'delayed:tasks',
        check_interval: float = 1.0
    ):
        """
        Initialize the task scheduler.
        
        Args:
            redis_host: Redis server hostname
            redis_port: Redis server port
            redis_db: Redis database number
            stream_name: Name of the Redis stream to use as the task queue
            delayed_key: Name of the Redis sorted set for delayed tasks
            check_interval: How often to check for due tasks, in seconds
        """
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.redis_db = redis_db
        self.stream_name = stream_name
        self.delayed_key = delayed_key
        self.check_interval = check_interval
        self.running = False
        self.client = self._create_redis_client()
        self.logger = logging.getLogger("TaskScheduler")
        
    def _create_redis_client(self) -> redis.Redis:
        """Create and return a Redis client."""
        return redis.Redis(
            host=self.redis_host,
            port=self.redis_port,
            db=self.redis_db,
            decode_responses=True
        )
        
    def start(self) -> None:
        """Start the scheduler thread."""
        if self.running:
            return
            
        self.running = True
        self.logger.info("Starting task scheduler")
        
        # Start the scheduler thread
        self.thread = threading.Thread(target=self._scheduler_loop, daemon=True)
        self.thread.start()
        
    def stop(self) -> None:
        """Stop the scheduler thread."""
        if not self.running:
            return
            
        self.logger.info("Stopping task scheduler")
        self.running = False
        
        # Wait for the thread to finish
        self.thread.join(timeout=5.0)
        
    def _scheduler_loop(self) -> None:
        """Main scheduler loop for checking and processing due tasks."""
        while self.running:
            try:
                now = time.time()
                
                # Find all tasks that are due (score <= current time)
                due_tasks = self.client.zrangebyscore(
                    self.delayed_key, 
                    0, 
                    now
                )
                
                if due_tasks:
                    self.logger.info(f"Found {len(due_tasks)} due tasks")
                    
                    # Process each due task
                    for task_json in due_tasks:
                        try:
                            # Parse the task data
                            task_data = json.loads(task_json)
                            task_id = task_data.get('task_id')
                            
                            if not task_id:
                                self.logger.warning(f"Invalid task format: {task_data}")
                                continue
                                
                            # Add the task to the main stream
                            # Convert all values to strings for Redis
                            stream_data = {
                                k: str(v) if not isinstance(v, str) else v 
                                for k, v in task_data.items()
                            }
                            
                            self.client.xadd(
                                self.stream_name,
                                stream_data,
                                id='*'
                            )
                            
                            # Update task status
                            self.client.hset(f"task_status:{task_id}", mapping={
                                'state': 'queued',
                                'queued_at': time.time()
                            })
                            
                            self.logger.info(f"Moved scheduled task {task_id} to the queue")
                            
                            # Remove the task from the delayed set
                            self.client.zrem(self.delayed_key, task_json)
                            
                        except json.JSONDecodeError:
                            self.logger.error(f"Invalid JSON in delayed task: {task_json}")
                            # Remove invalid entries
                            self.client.zrem(self.delayed_key, task_json)
                        except Exception as e:
                            self.logger.error(f"Error processing delayed task: {e}")
                
                # Sleep until next check interval
                time.sleep(self.check_interval)
                
            except Exception as e:
                self.logger.error(f"Error in scheduler loop: {e}")
                time.sleep(5)  # Avoid tight error loops 