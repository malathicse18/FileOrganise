import os
import shutil
import argparse
import logging
import time
import threading
import json
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from pymongo import MongoClient

# Logging Configuration
logging.basicConfig(
    filename="file_organization.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Persistent Task Storage File
TASK_FILE = "file_tasks.json"

# MongoDB Configuration
MONGO_URI = "mongodb://localhost:27017/"  # Update with your MongoDB URI
client = MongoClient(MONGO_URI)
db = client["file_organization_db"]
logs_collection = db["file_logs"]

# Initialize Scheduler
scheduler = BackgroundScheduler()

# Define folder names for different file types
FILE_TYPES = {
    "Images": [".jpg", ".jpeg", ".png", ".gif", ".bmp", ".tiff", ".svg"],
    "Videos": [".mp4", ".mkv", ".flv", ".mov", ".avi", ".wmv"],
    "Documents": [".pdf", ".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pptx", ".txt"],
    "Audio": [".mp3", ".wav", ".aac", ".flac", ".ogg"],
    "Archives": [".zip", ".rar", ".tar", ".gz", ".7z"],
    "Executables": [".exe", ".msi", ".bat", ".sh"],
    "Code": [".py", ".js", ".html", ".css", ".java", ".cpp", ".c", ".php"],
    "Data": [".csv", ".json", ".xml", ".sql", ".db"],
}

def log_to_mongodb(task_name, details, status, level="INFO"):
    """Log actions to MongoDB."""
    log_entry = {
        "task_name": task_name,
        "details": details,
        "status": status,
        "level": level,
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
    }
    logs_collection.insert_one(log_entry)

def load_tasks():
    """Load tasks from the JSON file."""
    try:
        with open(TASK_FILE, "r") as f:
            tasks = json.load(f)
            return tasks if isinstance(tasks, dict) else {}
    except (FileNotFoundError, json.JSONDecodeError):
        return {}

def save_tasks(tasks):
    """Save tasks to the JSON file."""
    with open(TASK_FILE, "w") as f:
        json.dump(tasks, f, indent=4)

def organize_files(directory):
    """Organize files in the given directory based on their extensions."""
    try:
        # Get a list of all files in the directory
        files = [f for f in os.listdir(directory) if os.path.isfile(os.path.join(directory, f))]
        
        for file in files:
            # Get the file extension
            file_extension = os.path.splitext(file)[1].lower()  # Ensure lowercase for consistency
            
            # Find the category for the file extension
            category = "Others"  # Default category for unknown file types
            for folder_name, extensions in FILE_TYPES.items():
                if file_extension in extensions:
                    category = folder_name
                    break
            
            # Create the category folder if it doesn't exist
            category_folder = os.path.join(directory, category)
            if not os.path.exists(category_folder):
                os.makedirs(category_folder)
            
            # Move the file to the category folder
            shutil.move(os.path.join(directory, file), os.path.join(category_folder, file))
            logging.info(f"Moved '{file}' to '{category}' folder.")
            log_to_mongodb("organize_files", {"file": file, "category": category}, "File moved")
        
        logging.info(f"File organization in '{directory}' completed successfully.")
        log_to_mongodb("organize_files", {"directory": directory}, "Organization completed")
    except Exception as e:
        logging.error(f"Error organizing files in '{directory}': {e}")
        log_to_mongodb("organize_files", {"directory": directory, "error": str(e)}, "Error", level="ERROR")

def add_task(interval, unit, directory):
    """Add a new file organization task to the scheduler."""
    tasks = load_tasks()
    task_name = f"task_{len(tasks) + 1}"

    new_task_details = {
        "interval": interval,
        "unit": unit,
        "directory": directory
    }

    # Check for duplicates
    for existing_task_name, existing_task_details in tasks.items():
        if (
            existing_task_details["interval"] == interval
            and existing_task_details["unit"] == unit
            and existing_task_details["directory"] == directory
        ):
            print("⚠️ Task with the same interval and directory already exists.")
            return

    # Add the new task
    tasks[task_name] = new_task_details
    save_tasks(tasks)

    trigger = IntervalTrigger(**{unit: interval})
    scheduler.add_job(organize_files, trigger, args=[directory], id=task_name)

    logging.info(f"Added task to organize '{directory}' every {interval} {unit}.")
    log_to_mongodb("add_task", {"directory": directory, "interval": interval, "unit": unit}, "Task added")
    print(f"✅ Task '{task_name}' added successfully.")

def list_tasks():
    """List all scheduled file organization tasks."""
    tasks = load_tasks()
    if not tasks:
        print("⚠️ No scheduled tasks found.")
        return
    
    print("\n📌 Scheduled File Organization Tasks:")
    for task_name, details in tasks.items():
        print(f"🔹 {task_name} - Every {details['interval']} {details['unit']}")

def remove_task(task_name):
    """Remove a scheduled file organization task."""
    tasks = load_tasks()
    if task_name not in tasks:
        print(f"⚠️ Task '{task_name}' not found.")
        return

    del tasks[task_name]
    save_tasks(tasks)

    try:
        scheduler.remove_job(task_name)
        logging.info(f"Removed task '{task_name}'.")
        log_to_mongodb("remove_task", {"task_id": task_name}, "Task removed")
        print(f"✅ Task '{task_name}' removed successfully.")
    except Exception as e:
        logging.error(f"Failed to remove task '{task_name}': {e}")
        log_to_mongodb("remove_task", {"task_id": task_name, "error": str(e)}, "Error", level="ERROR")
        print(f"⚠️ Task '{task_name}' not found.")

# CLI Argument Parsing
parser = argparse.ArgumentParser(description="File Organization Scheduler")
parser.add_argument("--add", type=int, help="Add a new task with interval")
parser.add_argument("--unit", type=str, choices=["seconds", "minutes", "hours", "days"], help="Time unit for the interval")
parser.add_argument("--directory", type=str, help="Directory to organize")
parser.add_argument("--list", action="store_true", help="List all scheduled tasks")
parser.add_argument("--remove", type=str, help="Remove a scheduled task by ID")

args = parser.parse_args()

if args.add:
    if not all((args.unit, args.directory)):
        print("⚠️ Please provide --unit and --directory.")
        exit(1)
    add_task(args.add, args.unit, args.directory)
elif args.list:
    list_tasks()
elif args.remove:
    remove_task(args.remove)

def load_and_schedule_tasks():
    """Load and schedule tasks from the JSON file."""
    tasks = load_tasks()
    for task_name, details in tasks.items():
        trigger = IntervalTrigger(**{details["unit"]: details["interval"]})
        scheduler.add_job(
            organize_files,
            trigger,
            args=[details["directory"]],
            id=task_name,
        )

# Load and schedule tasks before parsing commands
load_and_schedule_tasks()

def start_scheduler():
    """Runs the scheduler in a separate thread."""
    scheduler.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("🛑 Scheduler stopped.")
        scheduler.shutdown()

# Start the scheduler thread only if no other command is given
if not (args.add or args.list or args.remove):
    scheduler_thread = threading.Thread(target=start_scheduler, daemon=True)
    scheduler_thread.start()

    # Keep the main thread alive
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("🛑 Scheduler stopped.")
        scheduler.shutdown()
