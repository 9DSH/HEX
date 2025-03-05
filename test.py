
import threading


upload_event = threading.Event()

def generate_csv():
    upload_event.set()
    try:
        while True: 
            x = 2 * 4
            print(x)
    finally:
        upload_event.clear()

def start_upload_thread():
    if not upload_event.is_set():
        upload_thread = threading.Thread(target=generate_csv)  # Remove parentheses
        upload_thread.start()
    else:
        print("Upload thread is already running.")

start_upload_thread()