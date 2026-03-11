import os
import json

def count_backup_stats(root_dir):
    total_files = 0
    total_messages = 0
    total_chars = 0
    
    for root, dirs, files in os.walk(root_dir):
        for file in files:
            if file.endswith('.json') and not file.startswith('metadata'):
                file_path = os.path.join(root, file)
                total_files += 1
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                        # Assuming the JSON is a list of messages or contains a 'messages' key
                        messages = []
                        if isinstance(data, list):
                            messages = data
                        elif isinstance(data, dict):
                            # Try common keys
                            for key in ['messages', 'records', 'data']:
                                if key in data and isinstance(data[key], list):
                                    messages = data[key]
                                    break
                        
                        total_messages += len(messages)
                        for msg in messages:
                            content = ""
                            if isinstance(msg, dict):
                                content += str(msg.get('text', '')) or ""
                                content += str(msg.get('text_content', '')) or ""
                                content += str(msg.get('file_name', '')) or ""
                                content += str(msg.get('caption', '')) or ""
                            elif isinstance(msg, str):
                                content = msg
                            total_chars += len(content)
                except Exception as e:
                    # print(f"Error reading {file_path}: {e}")
                    pass
                    
    return total_files, total_messages, total_chars

if __name__ == "__main__":
    backup_root = r'f:\funny_project\tgporncopilot\data\archived\backups'
    files, msgs, chars = count_backup_stats(backup_root)
    print(f"Total Backup Files: {files}")
    print(f"Total Messages: {msgs}")
    print(f"Total Combined Characters: {chars}")
