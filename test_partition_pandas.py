import pytest
import pandas as pd
from src.partition_pandas import partition_dataframe

@pytest.fixture
def sample_df():
    """Создаёт тестовый DataFrame"""
    dfs = pd.date_range("2023-01-01 00:00:00", "2023-01-01 00:00:05", freq="s").repeat(3)
    return pd.DataFrame({"dt": dfs})

def test_partition_size(sample_df):
    """Проверяет, что чанки имеют нужный размер"""
    chunks = list(partition_dataframe(sample_df, "dt", 3))
    assert all(len(c) >= 3 for c in chunks), "Чанк меньше допустимого размера"
    assert sum(len(c) for c in chunks) == len(sample_df), "Потеря данных при разбиении"

def test_single_chunk(sample_df):
    """Тестирует случай, когда весь фрейм входит в один чанк"""
    chunks = list(partition_dataframe(sample_df, "dt", 100))
    assert len(chunks) == 1, "Ожидался один чанк"
    assert len(chunks[0]) == len(sample_df), "Размер чанка не совпадает с исходным фреймом"

def test_empty_df():
    """Тест на пустой датафрейм"""
    empty_df = pd.DataFrame({"dt": []})
    chunks = list(partition_dataframe(empty_df, "dt", 3))
    assert chunks == [], "Пустой фрейм должен давать пустой список чанков"

def test_large_df():
    """Тест на больших данных"""
    large_df = pd.DataFrame({"dt": pd.date_range("2023-01-01", periods=10**6, freq="ms")})
    chunks = list(partition_dataframe(large_df, "dt", 1000))
    assert sum(len(c) for c in chunks) == len(large_df), "Ошибка разбиения на больших данных"

def test_exact_chunk_size(sample_df):
    """Проверяет, что чанк создается ровно с min_chunk_size, если размер групп соответствует"""
    chunks = list(partition_dataframe(sample_df, "dt", 9))  # 3 повторения × 3 группы = 9
    assert all(len(c) == 9 for c in chunks), "Чанк должен быть ровно 9 строк"
    assert sum(len(c) for c in chunks) == len(sample_df), "Ошибка в суммарном количестве строк"

def test_more_groups_than_chunk_size():
    """Проверяет случай, когда групп больше, чем размер чанка"""
    df = pd.DataFrame({"dt": [f"2023-01-01 00:00:{i:02d}" for i in range(20)]})
    chunks = list(partition_dataframe(df, "dt", 5))
    assert len(chunks) > 1, "Ожидалось несколько чанков"
    assert sum(len(c) for c in chunks) == len(df), "Ошибка в разбиении данных"

def test_uneven_group_sizes():
    """Проверяет случай, когда группы имеют разный размер"""
    df = pd.DataFrame({"dt": ["A"] * 2 + ["B"] * 5 + ["C"] * 7})
    chunks = list(partition_dataframe(df, "dt", 6))
    assert len(chunks) == 2, "Ожидалось два чанка"
    assert sum(len(c) for c in chunks) == len(df), "Ошибка в суммарном количестве строк"

def test_non_string_group_column():
    """Тест на работу с числовым столбцом группировки"""
    df = pd.DataFrame({"id": [1, 2, 2, 3, 3, 3, 4, 4, 4, 4], "value": range(10)})
    chunks = list(partition_dataframe(df, "id", 4))
    assert all(len(c) >= 4 for c in chunks), "Ожидались чанки не менее 4 строк"

def test_large_chunk_size(sample_df):
    """Тест на чанк больше, чем размер DataFrame"""
    chunks = list(partition_dataframe(sample_df, "dt", 1000))
    assert len(chunks) == 1, "Ожидался один чанк"
    assert len(chunks[0]) == len(sample_df), "Ошибка в размере чанка"

def test_no_group_column():
    """Проверяет ошибку при отсутствии группирующего столбца"""
    df = pd.DataFrame({"other_column": [1, 2, 3]})
    try:
        list(partition_dataframe(df, "dt", 2))
        assert False, "Ожидалось исключение из-за отсутствия столбца 'dt'"
    except KeyError:
        pass  # Ожидаемое поведение

