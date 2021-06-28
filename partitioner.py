import pandas as pd


def strftime(date: pd.Timestamp):
    """Форматирует объект Timestamp в строку."""
    return date.strftime('%Y-%m-%d')

PARTITION_SIZES = {'day', 'month'}
day = pd.Timedelta(1, 'day')

def execute(partition_size,
            date_begin,
            date_end,
            db,
            table,
            slice_column):
    # Проверка на верность ввода
    assert partition_size in PARTITION_SIZES, f'Неверно указан partition_size. Возможные значения: {partition_sizes}.'

    date_begin, date_end = map(pd.Timestamp, [date_begin, date_end])

    # Если не месяц, сразу задаём размер шага
    if partition_size == 'day':
        timedelta = day

    partitions = []
    start = date_begin

    while start < date_end:
        # Делаем шаг размером в месяц, если месяц
        if partition_size == 'month':
            timedelta = day * start.days_in_month
        # Задаём конец интервала
        end = start + timedelta - day
        # Записываем начало и конец интервала
        partitions.append((strftime(start), strftime(end)))
        # Начинаем со следующей даты
        start += timedelta

    queries = []

    for start, end in partitions:
        query = f'''select * from {db}.{table}
        where {slice_column} between date '{start}' and date '{end}'\
        '''

        partition = f'''
        <Partition>
            <ID>{table.replace('_', ' ').upper()} {end}</ID>
            <Name>{table.replace('_', ' ').upper()} {end}</Name>
            <Source xsi:type="QueryBinding">
                <DataSourceID>DM_OLAP</DataSourceID>
                <QueryDefinition>
                {query}
                </QueryDefinition>
            </Source>
            <StorageMode>Molap</StorageMode>
            <ProcessingMode>Regular</ProcessingMode>
            <ProactiveCaching>
                <SilenceInterval>-PT1S</SilenceInterval>
                <Latency>-PT1S</Latency>
                <SilenceOverrideInterval>-PT1S</SilenceOverrideInterval>
                <Source xsi:type="ProactiveCachingInheritedBinding" />
            </ProactiveCaching>
        </Partition>'''
        queries.append(partition)

    result = '\n'.join(queries)

    return result
