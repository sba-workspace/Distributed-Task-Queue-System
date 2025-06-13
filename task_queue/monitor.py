import json
import time
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any

import redis


class TaskMonitor:
    """
    Monitor for observing and analyzing the state of the task queue.
    Provides methods to query queue length, pending tasks, consumer status, etc.
    """
    
    def __init__(
        self,
        redis_host: str = 'localhost',
        redis_port: int = 6379,
        redis_db: int = 0,
        stream_name: str = 'tasks',
        consumer_group: str = 'workers',
        delayed_key: str = 'delayed:tasks'
    ):
        """
        Initialize the task monitor.
        
        Args:
            redis_host: Redis server hostname
            redis_port: Redis server port
            redis_db: Redis database number
            stream_name: Name of the Redis stream to use as the task queue
            consumer_group: Name of the consumer group
            delayed_key: Name of the Redis sorted set for delayed tasks
        """
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.redis_db = redis_db
        self.stream_name = stream_name
        self.consumer_group = consumer_group
        self.delayed_key = delayed_key
        self.client = redis.Redis(
            host=self.redis_host,
            port=self.redis_port,
            db=self.redis_db,
            decode_responses=True
        )
        
    def get_queue_length(self) -> int:
        """
        Get the total number of entries in the task stream.
        
        Returns:
            Number of entries in the task stream
        """
        return self.client.xlen(self.stream_name)
        
    def get_pending_count(self) -> int:
        """
        Get the number of pending (unacknowledged) tasks.
        
        Returns:
            Number of pending tasks
        """
        try:
            # Get pending entries summary (total count is the first item)
            pending = self.client.xpending(self.stream_name, self.consumer_group)
            return pending['pending']
        except (redis.exceptions.ResponseError, KeyError):
            # Group might not exist yet
            return 0
            
    def get_delayed_count(self) -> int:
        """
        Get the number of delayed tasks.
        
        Returns:
            Number of delayed tasks
        """
        return self.client.zcard(self.delayed_key)
    
    def get_pending_tasks(self, count: int = 10) -> List[Dict[str, Any]]:
        """
        Get details of pending tasks.
        
        Args:
            count: Maximum number of pending tasks to return
            
        Returns:
            List of pending tasks with details
        """
        try:
            # Get detailed pending entries information
            pending_entries = self.client.xpending_range(
                self.stream_name,
                self.consumer_group,
                min='-',
                max='+',
                count=count
            )
            
            result = []
            for entry in pending_entries:
                # Add task details to the result
                task_info = {
                    'message_id': entry['message_id'],
                    'consumer': entry['consumer'],
                    'idle_time_ms': entry['idle'],
                    'delivery_count': entry['times_delivered'],
                }
                
                # Get the actual task data from the message
                message = self.client.xrange(
                    self.stream_name, 
                    min=entry['message_id'], 
                    max=entry['message_id']
                )
                if message:
                    task_data = message[0][1]
                    task_id = task_data.get('task_id')
                    if task_id:
                        task_info['task_id'] = task_id
                        task_info['task_type'] = task_data.get('task_type', 'unknown')
                        
                        # Get task status
                        status = self.client.hgetall(f"task_status:{task_id}")
                        if status:
                            task_info['state'] = status.get('state', 'unknown')
                
                result.append(task_info)
            
            return result
            
        except redis.exceptions.ResponseError:
            # Group might not exist yet
            return []
    
    def get_consumers(self) -> List[Dict[str, Any]]:
        """
        Get information about active consumers.
        
        Returns:
            List of consumer information
        """
        try:
            consumers_info = self.client.xinfo_consumers(self.stream_name, self.consumer_group)
            result = []
            
            for consumer in consumers_info:
                result.append({
                    'name': consumer['name'],
                    'pending': consumer['pending'],
                    'idle_time_ms': consumer['idle'],
                })
                
            return result
            
        except redis.exceptions.ResponseError:
            # Group might not exist yet
            return []
            
    def get_delayed_tasks(self, count: int = 10) -> List[Dict[str, Any]]:
        """
        Get scheduled/delayed tasks.
        
        Args:
            count: Maximum number of delayed tasks to return
            
        Returns:
            List of delayed tasks with details
        """
        # Get tasks sorted by execution time (earliest first)
        tasks_with_scores = self.client.zrange(
            self.delayed_key,
            0,
            count - 1,
            withscores=True
        )
        
        result = []
        for task_json, score in tasks_with_scores:
            try:
                # Parse the task data
                task_data = json.loads(task_json)
                
                # Create a record with execution time and task details
                task_info = {
                    'task_id': task_data.get('task_id', 'unknown'),
                    'task_type': task_data.get('task_type', 'unknown'),
                    'execute_at': score,
                    'execute_at_human': datetime.fromtimestamp(score).strftime('%Y-%m-%d %H:%M:%S'),
                    'wait_time_sec': max(0, score - time.time())
                }
                
                result.append(task_info)
                
            except json.JSONDecodeError:
                # Skip invalid entries
                pass
                
        return result
        
    def get_recent_tasks(self, count: int = 10) -> List[Dict[str, Any]]:
        """
        Get most recently added tasks from the stream.
        
        Args:
            count: Maximum number of tasks to return
            
        Returns:
            List of recent tasks
        """
        entries = self.client.xrevrange(self.stream_name, max='+', min='-', count=count)
        
        result = []
        for message_id, data in entries:
            task_id = data.get('task_id')
            if task_id:
                # Create task info record
                task_info = {
                    'message_id': message_id,
                    'task_id': task_id,
                    'task_type': data.get('task_type', 'unknown'),
                }
                
                # Get status
                status = self.client.hgetall(f"task_status:{task_id}")
                if status:
                    task_info['state'] = status.get('state', 'unknown')
                    
                    # Add timestamps
                    for time_field in ['created_at', 'started_at', 'completed_at']:
                        if time_field in status:
                            try:
                                timestamp = float(status[time_field])
                                task_info[time_field] = timestamp
                                task_info[f"{time_field}_human"] = datetime.fromtimestamp(
                                    timestamp
                                ).strftime('%Y-%m-%d %H:%M:%S')
                            except (ValueError, TypeError):
                                pass
                
                # Get result if completed
                if status and status.get('state') in ['completed', 'failed']:
                    result_data = self.client.hgetall(f"task_result:{task_id}")
                    if result_data:
                        task_info['success'] = result_data.get('success') == 'True'
                        if 'result' in result_data and result_data['result']:
                            try:
                                task_info['result'] = json.loads(result_data['result'])
                            except json.JSONDecodeError:
                                task_info['result'] = result_data['result']
                        if 'error' in result_data:
                            task_info['error'] = result_data['error']
                
                result.append(task_info)
                
        return result
        
    def get_task_by_id(self, task_id: str) -> Optional[Dict[str, Any]]:
        """
        Get detailed information about a specific task by its ID.
        
        Args:
            task_id: The ID of the task to look up
            
        Returns:
            Dictionary with task details or None if not found
        """
        # Get status data
        status = self.client.hgetall(f"task_status:{task_id}")
        if not status:
            return None
            
        # Create result with status data
        result = {
            'task_id': task_id,
            'state': status.get('state', 'unknown'),
        }
        
        # Add timestamps
        for time_field in ['created_at', 'started_at', 'completed_at', 'scheduled_for']:
            if time_field in status:
                try:
                    timestamp = float(status[time_field])
                    result[time_field] = timestamp
                    result[f"{time_field}_human"] = datetime.fromtimestamp(
                        timestamp
                    ).strftime('%Y-%m-%d %H:%M:%S')
                except (ValueError, TypeError):
                    pass
        
        # Add worker info if running
        if 'worker' in status:
            result['worker'] = status['worker']
            
        # Get result data if completed/failed
        if status.get('state') in ['completed', 'failed']:
            result_data = self.client.hgetall(f"task_result:{task_id}")
            if result_data:
                result['success'] = result_data.get('success') == 'True'
                if 'result' in result_data and result_data['result']:
                    try:
                        result['result'] = json.loads(result_data['result'])
                    except json.JSONDecodeError:
                        result['result'] = result_data['result']
                if 'error' in result_data:
                    result['error'] = result_data['error']
                    
        # Try to find the original task data in the stream
        # This is inefficient for large streams, but we're doing it only for one task
        entries = self.client.xrange(self.stream_name, min='-', max='+')
        for message_id, data in entries:
            if data.get('task_id') == task_id:
                result['message_id'] = message_id
                result['task_type'] = data.get('task_type', 'unknown')
                
                # Try to parse the payload
                if 'payload' in data:
                    try:
                        result['payload'] = json.loads(data['payload'])
                    except json.JSONDecodeError:
                        result['payload'] = data['payload']
                        
                break
                
        return result
        
    def get_queue_stats(self) -> Dict[str, Any]:
        """
        Get overview statistics for the task queue.
        
        Returns:
            Dictionary with queue statistics
        """
        stats = {
            'timestamp': time.time(),
            'timestamp_human': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'queue_length': self.get_queue_length(),
            'pending_tasks': self.get_pending_count(),
            'delayed_tasks': self.get_delayed_count(),
        }
        
        # Count tasks by state
        state_counts = {
            'queued': 0,
            'running': 0,
            'completed': 0,
            'failed': 0,
            'scheduled': 0
        }
        
        # Sample some recent tasks to estimate state distribution
        recent_tasks = self.get_recent_tasks(100)
        for task in recent_tasks:
            state = task.get('state')
            if state in state_counts:
                state_counts[state] += 1
                
        stats['tasks_by_state'] = state_counts
        
        # Get consumer information
        consumers = self.get_consumers()
        stats['active_consumers'] = len(consumers)
        stats['consumers'] = consumers
        
        return stats 