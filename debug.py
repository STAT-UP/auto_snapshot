import datetime

def debug_title(_text):
    print("####################################################################")
    print("## " + _text)
    print("####################################################################")

def debug_heartbeat():
    now = datetime.datetime.now().strftime("%Y-%m-%d_%H:%M")
    
    print(f"[{now}] Still alive")
