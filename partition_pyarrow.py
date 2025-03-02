import pyarrow as pa
import pyarrow.compute as pc
import tracemalloc

def partition_arrow_table(table: pa.Table, column: str, min_chunk_size: int):
    value_counts = pc.value_counts(table[column])

    unique_vals = value_counts.field('values')
    counts = value_counts.field('counts')

    chunk_start = 0
    chunk_size = 0
    idx = 0

    while idx < len(unique_vals):
        chunk_size += counts[idx].as_py()

        if chunk_size >= min_chunk_size:
            yield table.slice(chunk_start, chunk_size)
            chunk_start += chunk_size
            chunk_size = 0

        idx += 1

    if chunk_size > 0:
        yield table.slice(chunk_start)


if __name__ == "__main__":
    data = {
        'dt': [
            "2023-01-01 00:00:01", "2023-01-01 00:00:01", "2023-01-01 00:00:02",
            "2023-01-01 00:00:02", "2023-01-01 00:00:02", "2023-01-01 00:00:03"
        ]
    }
    table = pa.table(data)
    print("Исходная таблица PyArrow:")
    print(table)

    tracemalloc.start()
    before_mem = tracemalloc.get_traced_memory()[0]

    chunk_sizes = range(1, 7)

    for size in chunk_sizes:
        print(f"Чанки для min_chunk_size = {size}")
        for i, chunk in enumerate(partition_arrow_table(table, "dt", size), 1):
            print(f"Чанк {i}:")
            print(chunk)

    after_mem = tracemalloc.get_traced_memory()[0]
    tracemalloc.stop()

    print(f"Использовано памяти: {after_mem - before_mem} байт")
