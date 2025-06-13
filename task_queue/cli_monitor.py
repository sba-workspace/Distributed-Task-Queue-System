import argparse
import json
import os
import sys
import time
from datetime import datetime
try:
    from rich.console import Console
    from rich.table import Table
    from rich.live import Live
    from rich.panel import Panel
    from rich.layout import Layout
    HAS_RICH = True
except ImportError:
    HAS_RICH = False

from task_queue.monitor import TaskMonitor


class TaskQueueCLI:
    """
    Command-line interface for monitoring the task queue.
    """
    
    def __init__(
        self,
        redis_host: str = 'localhost',
        redis_port: int = 6379,
        redis_db: int = 0,
        stream_name: str = 'tasks',
        consumer_group: str = 'workers',
        delayed_key: str = 'delayed:tasks',
        refresh_interval: float = 2.0
    ):
        """
        Initialize the CLI monitor.
        
        Args:
            redis_host: Redis server hostname
            redis_port: Redis server port
            redis_db: Redis database number
            stream_name: Name of the Redis stream to use as the task queue
            consumer_group: Name of the consumer group
            delayed_key: Name of the Redis sorted set for delayed tasks
            refresh_interval: How often to refresh the display, in seconds
        """
        self.monitor = TaskMonitor(
            redis_host=redis_host,
            redis_port=redis_port,
            redis_db=redis_db,
            stream_name=stream_name,
            consumer_group=consumer_group,
            delayed_key=delayed_key
        )
        self.refresh_interval = refresh_interval
        
        # Initialize Rich console if available
        if HAS_RICH:
            self.console = Console()
        
    def show_stats(self) -> None:
        """Display basic queue statistics."""
        stats = self.monitor.get_queue_stats()
        
        if HAS_RICH:
            # Use Rich tables for nicer display
            table = Table(title="Queue Statistics")
            table.add_column("Metric", style="cyan")
            table.add_column("Value", style="green")
            
            table.add_row("Queue Length", str(stats['queue_length']))
            table.add_row("Pending Tasks", str(stats['pending_tasks']))
            table.add_row("Delayed Tasks", str(stats['delayed_tasks']))
            table.add_row("Active Consumers", str(stats['active_consumers']))
            table.add_row("Timestamp", stats['timestamp_human'])
            
            self.console.print(table)
            
            # Show task state distribution if available
            if 'tasks_by_state' in stats:
                state_table = Table(title="Tasks by State")
                state_table.add_column("State", style="cyan")
                state_table.add_column("Count", style="green")
                
                for state, count in stats['tasks_by_state'].items():
                    state_table.add_row(state, str(count))
                
                self.console.print(state_table)
                
        else:
            # Fallback to plain text
            print("\n=== Queue Statistics ===")
            print(f"Queue Length: {stats['queue_length']}")
            print(f"Pending Tasks: {stats['pending_tasks']}")
            print(f"Delayed Tasks: {stats['delayed_tasks']}")
            print(f"Active Consumers: {stats['active_consumers']}")
            print(f"Timestamp: {stats['timestamp_human']}")
            
            if 'tasks_by_state' in stats:
                print("\n=== Tasks by State ===")
                for state, count in stats['tasks_by_state'].items():
                    print(f"{state}: {count}")
            
            print("\n")
            
    def show_consumers(self) -> None:
        """Display information about active consumers."""
        consumers = self.monitor.get_consumers()
        
        if not consumers:
            print("No active consumers found.")
            return
            
        if HAS_RICH:
            # Use Rich table
            table = Table(title="Active Consumers")
            table.add_column("Name", style="cyan")
            table.add_column("Pending", style="green")
            table.add_column("Idle (ms)", style="yellow")
            
            for consumer in consumers:
                table.add_row(
                    consumer['name'],
                    str(consumer['pending']),
                    str(consumer['idle_time_ms'])
                )
                
            self.console.print(table)
            
        else:
            # Fallback to plain text
            print("\n=== Active Consumers ===")
            print(f"{'Name':<30} {'Pending':<10} {'Idle (ms)':<15}")
            print("-" * 55)
            
            for consumer in consumers:
                print(f"{consumer['name']:<30} {consumer['pending']:<10} {consumer['idle_time_ms']:<15}")
                
            print("\n")
            
    def show_pending_tasks(self) -> None:
        """Display information about pending tasks."""
        tasks = self.monitor.get_pending_tasks(20)
        
        if not tasks:
            print("No pending tasks found.")
            return
            
        if HAS_RICH:
            # Use Rich table
            table = Table(title="Pending Tasks")
            table.add_column("Task ID", style="cyan")
            table.add_column("Type", style="green")
            table.add_column("State", style="yellow")
            table.add_column("Consumer", style="magenta")
            table.add_column("Idle (ms)", style="blue")
            table.add_column("Deliveries", style="red")
            
            for task in tasks:
                table.add_row(
                    task.get('task_id', 'N/A'),
                    task.get('task_type', 'N/A'),
                    task.get('state', 'N/A'),
                    task.get('consumer', 'N/A'),
                    str(task.get('idle_time_ms', 'N/A')),
                    str(task.get('delivery_count', 'N/A'))
                )
                
            self.console.print(table)
            
        else:
            # Fallback to plain text
            print("\n=== Pending Tasks ===")
            print(f"{'Task ID':<36} {'Type':<15} {'State':<10} {'Consumer':<20} {'Idle (ms)':<10} {'Deliveries':<5}")
            print("-" * 100)
            
            for task in tasks:
                print(
                    f"{task.get('task_id', 'N/A'):<36} "
                    f"{task.get('task_type', 'N/A'):<15} "
                    f"{task.get('state', 'N/A'):<10} "
                    f"{task.get('consumer', 'N/A'):<20} "
                    f"{task.get('idle_time_ms', 'N/A'):<10} "
                    f"{task.get('delivery_count', 'N/A'):<5}"
                )
                
            print("\n")
            
    def show_recent_tasks(self) -> None:
        """Display information about recent tasks."""
        tasks = self.monitor.get_recent_tasks(20)
        
        if not tasks:
            print("No recent tasks found.")
            return
            
        if HAS_RICH:
            # Use Rich table
            table = Table(title="Recent Tasks")
            table.add_column("Task ID", style="cyan")
            table.add_column("Type", style="green")
            table.add_column("State", style="yellow")
            table.add_column("Created At", style="magenta")
            
            for task in tasks:
                created_at = task.get('created_at_human', 'N/A')
                table.add_row(
                    task.get('task_id', 'N/A'),
                    task.get('task_type', 'N/A'),
                    task.get('state', 'N/A'),
                    created_at
                )
                
            self.console.print(table)
            
        else:
            # Fallback to plain text
            print("\n=== Recent Tasks ===")
            print(f"{'Task ID':<36} {'Type':<15} {'State':<10} {'Created At':<20}")
            print("-" * 85)
            
            for task in tasks:
                created_at = task.get('created_at_human', 'N/A')
                print(
                    f"{task.get('task_id', 'N/A'):<36} "
                    f"{task.get('task_type', 'N/A'):<15} "
                    f"{task.get('state', 'N/A'):<10} "
                    f"{created_at:<20}"
                )
                
            print("\n")
            
    def show_delayed_tasks(self) -> None:
        """Display information about delayed/scheduled tasks."""
        tasks = self.monitor.get_delayed_tasks(20)
        
        if not tasks:
            print("No delayed tasks found.")
            return
            
        if HAS_RICH:
            # Use Rich table
            table = Table(title="Scheduled Tasks")
            table.add_column("Task ID", style="cyan")
            table.add_column("Type", style="green")
            table.add_column("Execute At", style="yellow")
            table.add_column("Wait (sec)", style="magenta")
            
            for task in tasks:
                table.add_row(
                    task.get('task_id', 'N/A'),
                    task.get('task_type', 'N/A'),
                    task.get('execute_at_human', 'N/A'),
                    f"{int(task.get('wait_time_sec', 0))}"
                )
                
            self.console.print(table)
            
        else:
            # Fallback to plain text
            print("\n=== Scheduled Tasks ===")
            print(f"{'Task ID':<36} {'Type':<15} {'Execute At':<20} {'Wait (sec)':<10}")
            print("-" * 85)
            
            for task in tasks:
                print(
                    f"{task.get('task_id', 'N/A'):<36} "
                    f"{task.get('task_type', 'N/A'):<15} "
                    f"{task.get('execute_at_human', 'N/A'):<20} "
                    f"{int(task.get('wait_time_sec', 0)):<10}"
                )
                
            print("\n")
    
    def show_task_details(self, task_id: str) -> None:
        """Display detailed information about a specific task."""
        task = self.monitor.get_task_by_id(task_id)
        
        if not task:
            print(f"Task {task_id} not found.")
            return
            
        if HAS_RICH:
            # Use Rich for nice formatting
            self.console.print(f"[bold cyan]Task Details: {task_id}[/bold cyan]")
            
            panel_content = []
            panel_content.append(f"[bold]Task Type:[/bold] {task.get('task_type', 'N/A')}")
            panel_content.append(f"[bold]State:[/bold] {task.get('state', 'N/A')}")
            
            # Add timestamps
            for time_field in ['created_at_human', 'started_at_human', 'completed_at_human', 'scheduled_for_human']:
                if time_field in task:
                    name = time_field.replace('_human', '').replace('_', ' ').title()
                    panel_content.append(f"[bold]{name}:[/bold] {task[time_field]}")
                    
            # Add worker if available
            if 'worker' in task:
                panel_content.append(f"[bold]Worker:[/bold] {task['worker']}")
                
            # Add result/error information
            if 'success' in task:
                panel_content.append(f"[bold]Success:[/bold] {task['success']}")
                
            if 'result' in task:
                result = task['result']
                if isinstance(result, (dict, list)):
                    result = json.dumps(result, indent=2)
                panel_content.append(f"[bold]Result:[/bold] {result}")
                
            if 'error' in task:
                panel_content.append(f"[bold]Error:[/bold] {task['error']}")
                
            # Add payload if available
            if 'payload' in task:
                payload = task['payload']
                if isinstance(payload, (dict, list)):
                    payload = json.dumps(payload, indent=2)
                panel_content.append(f"[bold]Payload:[/bold] {payload}")
                
            self.console.print(Panel("\n".join(panel_content), title="Task Details"))
            
        else:
            # Fallback to plain text
            print(f"\n=== Task Details: {task_id} ===")
            print(f"Task Type: {task.get('task_type', 'N/A')}")
            print(f"State: {task.get('state', 'N/A')}")
            
            # Add timestamps
            for time_field in ['created_at_human', 'started_at_human', 'completed_at_human', 'scheduled_for_human']:
                if time_field in task:
                    name = time_field.replace('_human', '').replace('_', ' ').title()
                    print(f"{name}: {task[time_field]}")
                    
            # Add worker if available
            if 'worker' in task:
                print(f"Worker: {task['worker']}")
                
            # Add result/error information
            if 'success' in task:
                print(f"Success: {task['success']}")
                
            if 'result' in task:
                result = task['result']
                if isinstance(result, (dict, list)):
                    result = json.dumps(result, indent=2)
                print(f"Result: {result}")
                
            if 'error' in task:
                print(f"Error: {task['error']}")
                
            # Add payload if available
            if 'payload' in task:
                payload = task['payload']
                if isinstance(payload, (dict, list)):
                    payload = json.dumps(payload, indent=2)
                print(f"Payload: {payload}")
                
            print("\n")
    
    def dashboard(self) -> None:
        """
        Show a live updating dashboard with queue statistics and tasks.
        Requires the Rich library.
        """
        if not HAS_RICH:
            print("The Rich library is required for the dashboard mode.")
            print("Please install it with: pip install rich")
            return
            
        # Create a layout for the dashboard
        layout = Layout()
        
        # Split the layout into sections
        layout.split(
            Layout(name="header", size=3),
            Layout(name="main", ratio=1),
            Layout(name="footer", size=1)
        )
        
        # Split the main section into columns
        layout["main"].split_row(
            Layout(name="left"),
            Layout(name="right")
        )
        
        # Split the left column into top and bottom
        layout["left"].split(
            Layout(name="stats", ratio=2),
            Layout(name="recent", ratio=3)
        )
        
        # Split the right column into top and bottom
        layout["right"].split(
            Layout(name="pending", ratio=2),
            Layout(name="delayed", ratio=3)
        )
        
        def generate_header() -> Panel:
            """Generate the header panel with timestamp."""
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            return Panel(f"[bold]Task Queue Dashboard[/bold] - Updated: {now}", border_style="green")
            
        def generate_stats() -> Panel:
            """Generate the queue statistics panel."""
            stats = self.monitor.get_queue_stats()
            
            content = []
            content.append(f"[bold]Queue Length:[/bold] {stats['queue_length']}")
            content.append(f"[bold]Pending Tasks:[/bold] {stats['pending_tasks']}")
            content.append(f"[bold]Delayed Tasks:[/bold] {stats['delayed_tasks']}")
            content.append(f"[bold]Active Consumers:[/bold] {stats['active_consumers']}")
            
            # Add task state distribution
            if 'tasks_by_state' in stats:
                content.append("\n[bold]Tasks by State:[/bold]")
                for state, count in stats['tasks_by_state'].items():
                    content.append(f"  {state}: {count}")
            
            # Add consumer details
            if 'consumers' in stats and stats['consumers']:
                content.append("\n[bold]Consumers:[/bold]")
                for consumer in stats['consumers']:
                    content.append(f"  {consumer['name']}: {consumer['pending']} pending, {consumer['idle_time_ms']}ms idle")
            
            return Panel("\n".join(content), title="Queue Statistics", border_style="blue")
            
        def generate_recent() -> Panel:
            """Generate the recent tasks panel."""
            tasks = self.monitor.get_recent_tasks(10)
            
            if not tasks:
                return Panel("No recent tasks", title="Recent Tasks", border_style="yellow")
                
            table = Table(show_header=True, header_style="bold")
            table.add_column("ID", width=10)
            table.add_column("Type", width=15)
            table.add_column("State", width=10)
            table.add_column("Created", width=20)
            
            for task in tasks:
                task_id = task.get('task_id', 'N/A')
                if len(task_id) > 8:
                    task_id = task_id[:8] + "…"
                    
                created_at = task.get('created_at_human', 'N/A')
                
                table.add_row(
                    task_id,
                    task.get('task_type', 'N/A'),
                    task.get('state', 'N/A'),
                    created_at
                )
                
            return Panel(table, title="Recent Tasks", border_style="yellow")
            
        def generate_pending() -> Panel:
            """Generate the pending tasks panel."""
            tasks = self.monitor.get_pending_tasks(10)
            
            if not tasks:
                return Panel("No pending tasks", title="Pending Tasks", border_style="red")
                
            table = Table(show_header=True, header_style="bold")
            table.add_column("ID", width=10)
            table.add_column("Type", width=15)
            table.add_column("Consumer", width=15)
            table.add_column("Idle (ms)", width=10)
            
            for task in tasks:
                task_id = task.get('task_id', 'N/A')
                if len(task_id) > 8:
                    task_id = task_id[:8] + "…"
                    
                consumer = task.get('consumer', 'N/A')
                if len(consumer) > 13:
                    consumer = consumer[:13] + "…"
                    
                table.add_row(
                    task_id,
                    task.get('task_type', 'N/A'),
                    consumer,
                    str(task.get('idle_time_ms', 'N/A'))
                )
                
            return Panel(table, title="Pending Tasks", border_style="red")
            
        def generate_delayed() -> Panel:
            """Generate the delayed tasks panel."""
            tasks = self.monitor.get_delayed_tasks(10)
            
            if not tasks:
                return Panel("No delayed tasks", title="Scheduled Tasks", border_style="magenta")
                
            table = Table(show_header=True, header_style="bold")
            table.add_column("ID", width=10)
            table.add_column("Type", width=15)
            table.add_column("Execute At", width=20)
            table.add_column("Wait (s)", width=10)
            
            for task in tasks:
                task_id = task.get('task_id', 'N/A')
                if len(task_id) > 8:
                    task_id = task_id[:8] + "…"
                    
                table.add_row(
                    task_id,
                    task.get('task_type', 'N/A'),
                    task.get('execute_at_human', 'N/A'),
                    str(int(task.get('wait_time_sec', 0)))
                )
                
            return Panel(table, title="Scheduled Tasks", border_style="magenta")
            
        def generate_footer() -> Panel:
            """Generate the footer panel with instructions."""
            return Panel("Press Ctrl+C to exit", border_style="green")
            
        def update_dashboard() -> Layout:
            """Update all panels in the dashboard."""
            layout["header"].update(generate_header())
            layout["stats"].update(generate_stats())
            layout["recent"].update(generate_recent())
            layout["pending"].update(generate_pending())
            layout["delayed"].update(generate_delayed())
            layout["footer"].update(generate_footer())
            return layout
            
        # Start the live display
        with Live(update_dashboard(), refresh_per_second=1/self.refresh_interval) as live:
            try:
                while True:
                    time.sleep(self.refresh_interval)
                    live.update(update_dashboard())
            except KeyboardInterrupt:
                pass


def main():
    """Command-line entry point."""
    parser = argparse.ArgumentParser(description="Task Queue Monitor")
    
    # Redis connection options
    parser.add_argument('--host', default='localhost', help="Redis host")
    parser.add_argument('--port', type=int, default=6379, help="Redis port")
    parser.add_argument('--db', type=int, default=0, help="Redis database number")
    
    # Queue configuration
    parser.add_argument('--stream', default='tasks', help="Redis stream name")
    parser.add_argument('--group', default='workers', help="Consumer group name")
    parser.add_argument('--delayed-key', default='delayed:tasks', 
                        help="Redis key for delayed tasks")
    
    # Display options
    parser.add_argument('--refresh', type=float, default=2.0,
                       help="Refresh interval in seconds")
    
    # Actions
    subparsers = parser.add_subparsers(dest='command', help="Command to run")
    
    # Stats command
    subparsers.add_parser('stats', help="Show queue statistics")
    
    # Consumers command
    subparsers.add_parser('consumers', help="Show active consumers")
    
    # Pending command
    subparsers.add_parser('pending', help="Show pending tasks")
    
    # Recent command
    subparsers.add_parser('recent', help="Show recent tasks")
    
    # Delayed command
    subparsers.add_parser('delayed', help="Show delayed tasks")
    
    # Task command
    task_parser = subparsers.add_parser('task', help="Show task details")
    task_parser.add_argument('task_id', help="ID of the task to show")
    
    # Dashboard command
    subparsers.add_parser('dashboard', help="Show live dashboard (requires rich library)")
    
    args = parser.parse_args()
    
    # Create the CLI monitor
    cli = TaskQueueCLI(
        redis_host=args.host,
        redis_port=args.port,
        redis_db=args.db,
        stream_name=args.stream,
        consumer_group=args.group,
        delayed_key=args.delayed_key,
        refresh_interval=args.refresh
    )
    
    # Execute the requested command
    if args.command == 'stats':
        cli.show_stats()
    elif args.command == 'consumers':
        cli.show_consumers()
    elif args.command == 'pending':
        cli.show_pending_tasks()
    elif args.command == 'recent':
        cli.show_recent_tasks()
    elif args.command == 'delayed':
        cli.show_delayed_tasks()
    elif args.command == 'task':
        cli.show_task_details(args.task_id)
    elif args.command == 'dashboard':
        cli.dashboard()
    else:
        # Default to stats if no command specified
        cli.show_stats()


if __name__ == "__main__":
    main()