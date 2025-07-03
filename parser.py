import re
import sys
from collections import defaultdict

def normalize_query(query):
    """Нормализация SQL-запроса: замена литералов на плейсхолдеры."""
    query = re.sub(r'--.*', '', query)      # Удаление комментариев
    query = re.sub(r"'.*?'", '?', query)   # Замена строк
    query = re.sub(r'\b\d+\b', '?', query) # Замена чисел
    return query.strip()

def parse_log_in_chunks(file_path, chunk_size=1024*1024):
    """
    Парсинг лога порциями для экономии памяти.
    chunk_size - размер порции в байтах (по умолчанию 1MB)
    """
    timestamp_regex = re.compile(r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2})')
    counter = defaultdict(int)
    buffer = ""
    current_time = None
    current_query = []

    with open(file_path, 'r') as file:
        while True:
            chunk = file.read(chunk_size)
            if not chunk:  # Конец файла
                break
            
            buffer += chunk
            lines = buffer.split('\n')
            buffer = lines.pop()  # Последняя строка (возможно неполная)

            for line in lines:
                process_line(line, timestamp_regex, counter, current_query, current_time)
    
    # Обработка оставшегося буфера
    if buffer:
        process_line(buffer, timestamp_regex, counter, current_query, current_time)
    
    # Финализация последнего запроса
    if current_query:
        finalize_query(counter, current_query, current_time)
    
    return counter

def process_line(line, timestamp_regex, counter, current_query, current_time):
    """Обработка одной строки лога"""
    time_match = timestamp_regex.search(line)
    if "LOG:  statement: " in line and time_match:
        if current_query:
            finalize_query(counter, current_query, current_time)
        current_time = time_match.group(1)
        start = line.find("LOG:  statement: ") + len("LOG:  statement: ")
        current_query.append(line[start:].strip())
    elif time_match and current_query:
        finalize_query(counter, current_query, current_time)
        current_time = None
    elif current_query:
        current_query.append(line.strip())

def finalize_query(counter, current_query, current_time):
    """Финализация и нормализация текущего запроса"""
    if current_query:
        query_str = ' '.join(current_query)
        norm_query = normalize_query(query_str)
        counter[(current_time, norm_query)] += 1
        current_query.clear()

def main():
    if len(sys.argv) < 2:
        print("Использование: python parser.py <путь_к_файлу_лога> [размер_порции_в_MB]")
        sys.exit(1)
    
    chunk_size = 1024 * 1024  # 1MB по умолчанию
    if len(sys.argv) > 2:
        try:
            chunk_size = int(float(sys.argv[2]) * 1024 * 1024)
        except ValueError:
            print("Ошибка: размер порции должен быть числом (в MB)")
            sys.exit(1)
    
    counter = parse_log_in_chunks(sys.argv[1], chunk_size)
    
    print("Время,Запрос,Количество")
    for (time, query), count in sorted(counter.items()):
        print(f'"{time}","{query}",{count}')

if __name__ == "__main__":
    main()