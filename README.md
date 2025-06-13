# Distributed Task Queue System

A robust, distributed task queue system built with Python and Redis Streams. This system provides a reliable way to queue and process tasks across multiple workers with features like task scheduling, retries, and monitoring.

## Features

- **Redis Streams** as the backbone for reliable messaging
- **Consumer Groups** for distributed task processing
- **Multi-threading** and **Multi-processing** options for different workload types
- **Delayed/Scheduled Tasks** using Redis Sorted Sets
- **Automatic Retries** and fault tolerance for failed tasks
- **Task Monitoring** with a CLI dashboard
- **Result Storage** for later retrieval

## Installation

### Requirements

- Python 3.6+
- Redis 5.0+ (with Streams support)

### Setup

1. Clone this repository:
   ```
   git clone https://github.com/yourusername/distributed-task-system.git
   cd distributed-task-system
   ```

2. Install dependencies:
   ```
   pip install -r requirements.txt
   ```

3. Make sure Redis is running:
   ```
   # Start Redis locally (or use Docker)
   redis-server
   ```

## Quick Start

### Basic Usage

Here's a simple example of how to use the task queue:

```python
from task_queue.producer import TaskProducer
from task_queue.worker import TaskWorker

# Define a task handler
def process_data(payload):
    data = payload.get('data')
    result = data * 2
    return {'processed_data': result}

# Create a worker with the task handler
worker = TaskWorker(
    task_handlers={'process_data': process_data},
    thread_count=2
)
worker.start()

# Add a task to the queue
producer = TaskProducer()
task_id = producer.add_task(
    'process_data',
    {'data': 42}
)

print(f"Added task with ID: {task_id}")
```

### Running the Examples

Check out the example scripts in the `examples/` directory:

1. Basic usage:
   ```
   python examples/basic_usage.py
   ```

2. Multi-process workers (for CPU-bound tasks):
   ```
   python examples/multiprocess_workers.py
   ```

3. Monitor demo (with dashboard):
   ```
   python examples/monitor_demo.py
   ```

## Components

### TaskProducer

The `TaskProducer` class is used to add tasks to the queue:

```python
producer = TaskProducer(redis_host='localhost', redis_port=6379)

# Add a task for immediate execution
task_id = producer.add_task('task_type', {'key': 'value'})

# Schedule a task for future execution
import time
future_time = time.time() + 60  # 60 seconds from now
task_id = producer.schedule_task('task_type', {'key': 'value'}, execute_at=future_time)
```

### TaskWorker

The `TaskWorker` class processes tasks from the queue:

```python
def my_task_handler(payload):
    # Process the task
    return {'result': 'success'}

# Create a worker with handlers for different task types
worker = TaskWorker(
    task_handlers={
        'my_task': my_task_handler,
        'other_task': other_handler
    },
    thread_count=4  # Use 4 threads
)

# Start the worker
worker.start()

# When done
worker.stop()
```

### TaskScheduler

The `TaskScheduler` moves delayed tasks to the main queue when they're due:

```python
scheduler = TaskScheduler()
scheduler.start()

# When done
scheduler.stop()
```

### TaskMonitor

The `TaskMonitor` provides methods to query the state of the task queue:

```python
from task_queue.monitor import TaskMonitor

monitor = TaskMonitor()

# Get queue statistics
stats = monitor.get_queue_stats()
print(f"Queue length: {stats['queue_length']}")

# Get recent tasks
recent_tasks = monitor.get_recent_tasks(10)
for task in recent_tasks:
    print(f"Task {task['task_id']}: {task['state']}")
```

### CLI Monitor

The CLI monitor provides a command-line interface for monitoring the task queue:

```
python -m task_queue.cli_monitor dashboard
```

Available commands:
- `stats`: Show queue statistics
- `consumers`: Show active consumers
- `pending`: Show pending tasks
- `recent`: Show recent tasks
- `delayed`: Show delayed tasks
- `task <task_id>`: Show task details
- `dashboard`: Show live dashboard (requires Rich library)

## Architecture

The system uses Redis Streams as the primary data structure for the task queue. Here's how it works:

1. **Producers** add tasks to a Redis Stream using `XADD`
2. **Workers** read tasks from the stream as part of a consumer group using `XREADGROUP`
3. The **Scheduler** moves tasks from a sorted set to the stream when they're due
4. Workers **acknowledge** processed tasks with `XACK`
5. Failed tasks either remain in the pending entries list or are re-added with incremented retry count
6. The **Monitor** provides visibility into the queue state

## Configuration

Each component accepts configuration parameters for Redis connection, stream names, etc. The defaults are:

- Redis Host: `localhost`
- Redis Port: `6379`
- Redis DB: `0`
- Stream Name: `tasks`
- Consumer Group: `workers`
- Delayed Tasks Key: `delayed:tasks`

## Deployment Considerations

For production deployment:

1. **Redis Persistence**: Enable AOF persistence in Redis
2. **High Availability**: Consider using Redis Sentinel or Cluster
3. **Worker Scaling**: Run multiple worker processes (one per CPU core for CPU-bound tasks)
4. **Monitoring**: Use the monitoring tools to track queue health

## License

This project is licensed under the MIT License - see the LICENSE file for details. 