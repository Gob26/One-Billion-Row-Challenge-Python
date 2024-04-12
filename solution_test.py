import asyncio
import random
import os
from concurrent.futures import ProcessPoolExecutor
import time
import subprocess
from pathlib import Path


async def generate_test_file(file_path, num_lines):
    file_content = "\n".join([
        f"{random.choice(['Devi Hosūr', 'Taubaté', 'Tabūk', 'Agrigento', 'Srbobran'])};{round(random.uniform(-99.9, 99.9), 1)}"
        for _ in range(num_lines)])
    Path(file_path).write_text(file_content)
def process_chunk(chunk):
    stats = {}
    for line in chunk:
        station, temperature = line.strip().split(";")
        temperature = float(temperature)
        if station not in stats:
            stats[station] = {'max': float('-inf'), 'min': float('inf'), 'sum': 0, 'count': 0}
        stats[station]['max'] = max(stats[station]['max'], temperature)
        stats[station]['min'] = min(stats[station]['min'], temperature)
        stats[station]['sum'] += temperature
        stats[station]['count'] += 1
    return stats

async def process_file(file_path):
    stats = {}
    total_lines_processed = 0
    start_time = time.time()

    chunk = Path(file_path).read_text().splitlines()

    num_workers = 4
    chunk_size = len(chunk) // num_workers
    chunks = [chunk[i:i + chunk_size] for i in range(0, len(chunk), chunk_size)]

    with ProcessPoolExecutor(max_workers=num_workers) as executor:
        loop = asyncio.get_event_loop()
        tasks = [loop.run_in_executor(executor, process_chunk, chunk) for chunk in chunks]
        results = await asyncio.gather(*tasks)

    for result in results:
        for station, data in result.items():
            if station not in stats:
                stats[station] = data
            else:
                stats[station]['max'] = max(stats[station]['max'], data['max'])
                stats[station]['min'] = min(stats[station]['min'], data['min'])
                stats[station]['sum'] += data['sum']
                stats[station]['count'] += data['count']
        total_lines_processed += len(chunk)

    elapsed_time = time.time() - start_time
    speed = total_lines_processed / elapsed_time
    print(f"Processed {total_lines_processed} lines. Speed: {speed:.2f} lines per second")

    # Сортируем статистику по наименованию метеостанции
    sorted_stats = sorted(stats.items(), key=lambda x: x[0])

    with open('ft', 'w') as f:
        for station, values in sorted_stats:
            avg_temperature = round(values['sum'] / values['count'], 1) if values['count'] > 0 else 0
            min_temperature = round(values['min'], 1)
            max_temperature = round(values['max'], 1)
            # Записываем результаты в файл в требуемом формате
            f.write(f"{station}: {avg_temperature:.1f}, {min_temperature:.1f}, {max_temperature:.1f}\n")

async def main():
    subprocess.run(["python", "create_measures.py"])

    test_file_path = "measurements.txt"
    await generate_test_file(test_file_path, 1000000000)
    await process_file(test_file_path)

if __name__ == "__main__":
    asyncio.run(main())
