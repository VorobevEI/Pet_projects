import pandas as pd
import tracemalloc

def partition_dataframe(df: pd.DataFrame, column: str, min_chunk_size: int):
    grouped = df.groupby(column, sort=False)
    chunk, chunk_size = [], 0

    for _, group in grouped:
        chunk.append(group)
        chunk_size += len(group)

        if chunk_size >= min_chunk_size:
            yield pd.concat(chunk, copy=False)
            chunk, chunk_size = [], 0

    if chunk:
        yield pd.concat(chunk, copy=False)


if __name__ == "__main__":
    df = pd.DataFrame({"dt": pd.to_datetime([
        "2023-01-01 00:00:01",
        "2023-01-01 00:00:01",
        "2023-01-01 00:00:02",
        "2023-01-01 00:00:02",
        "2023-01-01 00:00:02",
        "2023-01-01 00:00:03",
    ])})

    print("Исходный DataFrame:")
    print(df)

    tracemalloc.start()
    before_mem = tracemalloc.get_traced_memory()[0]

    chunk_sizes = range(1, 7)

    for size in chunk_sizes:
        print(f"Чанки для min_chunk_size = {size}")
        for i, chunk in enumerate(partition_dataframe(df, "dt", size), 1):
            print(f"Чанк {i}:")
            print(chunk)

    after_mem = tracemalloc.get_traced_memory()[0]
    tracemalloc.stop()

    print(f"Использовано памяти: {after_mem - before_mem} байт")