from datetime import datetime

def log_operation(message, log_path):
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    with open(log_path, 'a') as f:
        f.write(timestamp + ' ' + message + '\n')