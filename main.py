import async_fun as sf
import pandas as pd
import time
import asyncio

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.max_colwidth', None)
pd.set_option('display.precision', 2)
pd.set_option('display.expand_frame_repr', False)

TEST = 0 
QUANTITY_OF_BLOCKS_IN_ITERATION = 30
MAX_ITERATIONS = 1000
REQUESTS_QUANTITY = 1000
MIN_VALUE_THRESHOLD = 0.01
MAX_LINES_IN_TX_CACHE = 200000
MAX_LINES_IN_HASH_CACHE = 10
START_BLOCK = 739999
BTC_PRICE_DIR = 'Traider_bot\\bot, scripts, data\\парсинг транзакций\\bitcoin_price\\BTCBUSD.csv.gz'
DATA_DIRECTORY = 'Traider_bot\\bot, scripts, data\\парсинг транзакций\\parsing\\data'

async def main():
    last_block = sf.get_existing_last_block(DATA_DIRECTORY, START_BLOCK)
    rpc_connection_main = sf.get_rpc_connection()
    all_blocks_to_download = sf.get_list_of_blocks_to_download(last_block, rpc_connection_main)
    blocks_to_parsing_generator = sf.split_list_into_chunks(all_blocks_to_download, QUANTITY_OF_BLOCKS_IN_ITERATION, MAX_ITERATIONS)
    tasks = []
    for blocks_group in blocks_to_parsing_generator:
        start_time = time.time()
        data = await sf.parsing_data(blocks_group)
        
        min_block_height, max_block_height = sf.get_statistik_data(data)
        save_task = asyncio.create_task(sf.async_save_data_to_parquet(data, min_block_height, max_block_height))
        tasks.append(save_task)
        time_of_circle = sf.print_cicle_info(start_time, min_block_height, max_block_height , len(blocks_group), data, QUANTITY_OF_BLOCKS_IN_ITERATION)
        sf.get_avg_blocks_in_minut(time_of_circle)





# транзакции с большим Transactions_Count - это батчевые 

if __name__ == "__main__":
    asyncio.run(main())

# есть теория что при большом проценте загрузок из кэша часть транзакций теряется

# попробовать написать либо асинхронный скрипт, либо многопоточный с локом соединения
# написать скрипт верификации данных в файлах
# поиск дублей

# Сохранено 743483 - 743507
# Обработка завершена, данные сохранены. Время выполнения: 103.80 секунд
# Блоков в минуту: 14.56
# Количество блоков для скачивания: 25

# Сохранено 743408 - 743432
# Обработка завершена, данные сохранены. Время выполнения: 116.26 секунд
# Блоков в минуту: 12.93
# Количество блоков для скачивания: 25

# Сохранено 743033 - 743057
# Обработка завершена, данные сохранены. Время выполнения: 136.76 секунд
# Блоков в минуту: 11.03
# Количество блоков для скачивания: 25

# Сохранено 742808 - 742832
# Обработка завершена, данные сохранены. Время выполнения: 163.30 секунд
# Блоков в минуту: 9.20
# Количество блоков для скачивания: 25
# Обработка завершена, данные сохранены.

# Сохранено 740908 - 740957371887207
# Обработка завершена, данные сохранены. Время выполнения: 441.09 секунд
# Блоков в минуту: 6.80
# Количество блоков для скачивания: 50


# Сохранено 740858 - 740907
# Обработка завершена, данные сохранены. Время выполнения: 475.16 секунд
# Блоков в минуту: 6.32
# Количество блоков для скачивания: 50


# c батчем
# Сохранено 740000 - 740011                           
# Обработка завершена, данные сохранены. Время выполнения: 368.99 секунд
# Блоков в минуту: 1.96
# Количество блоков для скачивания: 12
# Обработка завершена, данные сохранены.

# с батчем и фильтром транзакций >v0.01 BTC
# Сохранено 740000 - 740019 10221
# Обработка завершена, данные сохранены. Время выполнения: 414.67 секунд
# Блоков в минуту: 2.90
# Количество блоков для скачивания: 20
# Обработка завершена, данные сохранены.

# block_data_740080-740089.parquet 
# с кэшэм
# Блоков в минуту: 1.41
# Количество блоков для скачивания: 10

# батч, кэш хэшей блоков, проверка на уже скачанные tx(доделать последнее) и наверн все это можно попробовать в асинх но я не увеерн
# Сохранено 740100 - 740108
# Обработка завершена, данные сохранены. Время выполнения: 124.80 секунд
# Блоков в минуту: 4.84
# Количество блоков для скачивания: 10

# таже самая конфигурация 
# Сохранено 740149 - 740168- удалить проверку
# Обработка завершена, данные сохранены. Время выполнения: 176.32 секунд
# Блоков в минуту: 6.82
# Количество блоков для скачивания: 20






# посмотреть сколько времени занимают всякие функции - как минимум - проверка на дубли
# разобраитьтся с транзакциями из других блоков +
# может попробовать не пандас а еще что нибудь
# надо добавлять блокхэш при запросе транзакций, сделать кэш по ним !

# еще идеи для ускорения https://bitcoin.stackexchange.com/questions/89066/how-to-scale-json-rpc


# что бы найти кошельки который гоняют крипту между собой и биржей. или наверно найти кошельки бирж и скачать конкретно их
# потом отфильровать получившиеся блоки тип оставить 99%, что бы срезать те транзакции которые произошли давно.