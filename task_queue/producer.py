import json
import time
import uuid
from typing import Any, Dict, Optional

import redis


class TaskProducer:
    """
    Producer for adding tasks to a Redis Stream queue.
    """
    
    def __init__(
        self, 
        redis_host: str = 'localhost', 
        redis_port: int = 6379, 
        redis_db: int = 0,
        stream_name: str = 'tasks',
        delayed_key: str = 'delayed:tasks'
    ):
        """
        Initialize the task producer.
        
        Args:
            redis_host: Redis server hostname
            redis_port: Redis server port
            redis_db: Redis database number
            stream_name: Name of the Redis stream to use as the task queue
            delayed_key: Name of the Redis sorted set for delayed tasks
        """
        self.client = redis.Redis(
            host=redis_host,
            port=redis_port,
            db=redis_db,
            decode_responses=True
        )
        self.stream_name = stream_name
        self.delayed_key = delayed_key
    
    def add_task(
        self, 
        task_type: str, 
        payload: Dict[str, Any], 
        task_id: Optional[str] = None
    ) -> str:
        """
        Add a task to the queue.
        
        Args:
            task_type: Type of task to execute
            payload: Task payload/parameters as a dictionary
            task_id: Optional task ID (UUID generated if not provided)
            
        Returns:
            The task ID
        """
        if task_id is None:
            task_id = str(uuid.uuid4())
            
        # Prepare the task data
        task_data = {
            'task_id': task_id,
            'task_type': task_type,
            'payload': json.dumps(payload),
            'created_at': time.time(),
            'attempts': 0
        }
        
        # Add to the Redis stream
        self.client.xadd(
            self.stream_name,
            task_data,
            id='*'  # Auto-generate a message ID
        )
        
        # Set initial status in a hash
        status_data = {
            'state': 'queued',
            'created_at': task_data['created_at'],
            'worker': '',
        }
        self.client.hset(f'task_status:{task_id}', mapping=status_data)
        
        return task_id
        
    def schedule_task(
        self, 
        task_type: str, 
        payload: Dict[str, Any], 
        execute_at: float,
        task_id: Optional[str] = None
    ) -> str:
        """
        Schedule a task to run at a future time.
        
        Args:
            task_type: Type of task to execute
            payload: Task payload/parameters as a dictionary
            execute_at: Timestamp (in seconds since epoch) when the task should execute
            task_id: Optional task ID (UUID generated if not provided)
            
        Returns:
            The task ID
        """
        if task_id is None:
            task_id = str(uuid.uuid4())
            
        # Prepare the task data
        task_data = {
            'task_id': task_id,
            'task_type': task_type,
            'payload': json.dumps(payload),
            'scheduled_at': time.time(),
            'execute_at': execute_at,
            'attempts': 0
        }
        
        # Convert to JSON for storage in the sorted set
        task_json = json.dumps(task_data)
        
        # Add to the delayed tasks sorted set with score = execution time
        self.client.zadd(self.delayed_key, {task_json: execute_at})
        
        # Set initial status
        status_data = {
            'state': 'scheduled',
            'created_at': task_data['scheduled_at'],
            'scheduled_for': execute_at,
            'worker': '',
        }
        self.client.hset(f'task_status:{task_id}', mapping=status_data)
        
        return task_id 