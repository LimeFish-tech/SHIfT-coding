import re
import os
import glob
import tarfile
from datetime import datetime
from collections import defaultdict

class PostgresLogParser:
    def __init__(self, log_file_path):
        self.log_file_path = log_file_path
        self.log_pattern = re.compile(
            r'^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(?:\.\d+)?(?: UTC)?).*?statement: (.*?)(?:;|$)',
            re.IGNORECASE
        )
        self.string_pattern = re.compile(r"'(?:''|[^'])*'")
        self.number_pattern = re.compile(r'\b\d+\b')
        self.float_pattern = re.compile(r'\b\d+\.\d+\b')
        self.dates_seen = set()
        self.sql_cache = {}
        self.file_buffers = defaultdict(list)
        self.buffer_size = 1000
        self.run_timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        self.base_name = os.path.splitext(os.path.basename(log_file_path))[0]
        self.archive_name = f"{self.base_name}-{self.run_timestamp}.tar.gz"

    def parse(self):
        output_files = {}
        current_query = None

        try:
            with open(self.log_file_path, 'r', encoding='utf-8', errors='replace') as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue

                    if line[0].isdigit():
                        if current_query:
                            self.buffer_query(current_query, output_files)
                        current_query = self.extract_query(line)
                    elif current_query:
                        current_query['sql'] += ' ' + line
        finally:
            if current_query:
                self.buffer_query(current_query, output_files)
            self.flush_buffers(output_files)
            for file in output_files.values():
                file.close()

            self.generate_daily_summary()
            self.compress_results()
            self.cleanup_files()

    def extract_query(self, line):
        match = self.log_pattern.match(line)
        if not match:
            return None

        timestamp, sql = match.groups()
        return {'timestamp': timestamp.strip(), 'sql': sql.strip()}

    def buffer_query(self, query, output_files):
        if not query or 'sql' not in query:
            return

        sql = query['sql'].rstrip(';')
        first_word = sql.split()[0].upper() if sql else ''
        
        if first_word == 'WITH':
            operator = 'SELECT'
        elif first_word in ('SELECT', 'INSERT', 'UPDATE', 'DELETE'):
            operator = first_word
        else:
            operator = 'OTHER'

        date_part = query['timestamp'].split()[0]
        self.dates_seen.add(date_part)

        filename = f"{operator}_{date_part}.log"
        if filename not in output_files:
            output_files[filename] = open(filename, 'a', encoding='utf-8')

        self.file_buffers[filename].append(f"{query['timestamp']} | {sql}\n")
        if len(self.file_buffers[filename]) >= self.buffer_size:
            output_files[filename].writelines(self.file_buffers[filename])
            self.file_buffers[filename] = []

    def flush_buffers(self, output_files):
        for filename, buffer in self.file_buffers.items():
            if buffer and filename in output_files:
                output_files[filename].writelines(buffer)
        self.file_buffers.clear()

    def normalize_sql(self, sql):
        if sql in self.sql_cache:
            return self.sql_cache[sql]
        
        normalized = self.string_pattern.sub('$str', sql)
        normalized = self.float_pattern.sub('$num', normalized)
        normalized = self.number_pattern.sub('$num', normalized)
        normalized = ' '.join(normalized.split())
        
        self.sql_cache[sql] = normalized
        return normalized

    def generate_daily_summary(self):
        stats = defaultdict(lambda: defaultdict(int))
        cache = self.sql_cache

        for date in self.dates_seen:
            for file_path in glob.glob(f'*_{date}.log'):
                with open(file_path, 'r', encoding='utf-8') as f:
                    for line in f:
                        parts = line.split(' | ', 1)
                        if len(parts) < 2:
                            continue

                        timestamp, sql = parts
                        sql = sql.strip()
                        
                        if sql in cache:
                            normalized_sql = cache[sql]
                        else:
                            normalized_sql = self.normalize_sql(sql)
                        
                        try:
                            date_key = timestamp.split()[0]
                        except (ValueError, IndexError):
                            continue

                        stats[date_key][normalized_sql] += 1

        with open("daily_summary.log", 'w', encoding='utf-8') as sf:
            for day in sorted(stats):
                for sql, count in sorted(stats[day].items()):
                    sf.write(f"{day} | {sql} | выполнился {count} раз\n")

    def compress_results(self):
        files_to_archive = []
        summary_file = "daily_summary.log"
        if os.path.exists(summary_file):
            files_to_archive.append(summary_file)
        
        for date in self.dates_seen:
            for operator in ['SELECT', 'INSERT', 'UPDATE', 'DELETE', 'OTHER']:
                filename = f"{operator}_{date}.log"
                if os.path.exists(filename):
                    files_to_archive.append(filename)
        
        if files_to_archive:
            with tarfile.open(self.archive_name, "w:gz") as tar:
                for file in files_to_archive:
                    tar.add(file)
            print(f"Результаты сохранены в архив: {self.archive_name}")

    def cleanup_files(self):
        for file in glob.glob("*.log"):
            if file != self.log_file_path and file != self.archive_name:
                try:
                    os.remove(file)
                except Exception as e:
                    print(f"Ошибка при удалении файла {file}: {e}")

if __name__ == '__main__':
    import sys
    if len(sys.argv) != 2:
        print("Использование: python log_parser.py <путь_к_файлу_лога>")
        sys.exit(1)

    parser = PostgresLogParser(sys.argv[1])
    parser.parse()
