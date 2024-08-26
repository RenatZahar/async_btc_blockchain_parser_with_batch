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
QUANTITY_OF_BLOCKS_IN_ITERATION = 5
MAX_ITERATIONS = 10000 
REQUESTS_QUANTITY = 800
MIN_VALUE_THRESHOLD = 0.01
MAX_LINES_IN_TX_CACHE = 500000
MAX_LINES_IN_HASH_CACHE = 10
START_BLOCK = 740000
BTC_PRICE_DIR = r'I:\my_python\Traider_bot\bot, scripts, data\парсинг транзакций\bitcoin_price\BTCBUSD.csv.gz'
DATA_DIRECTORY = r'I:\my_python\Traider_bot\bot, scripts, data\парсинг транзакций\parsing\data'

# если биткоин кор зависает - уменьшаем блоки в чанке

PROBLEM_BLOCKS_LIST = 0 

async def main():
    last_block = sf.get_existing_last_block(DATA_DIRECTORY, START_BLOCK)
    rpc_connection_main = sf.get_rpc_connection()
    all_blocks_to_download = sf.get_list_of_blocks_to_download(last_block, rpc_connection_main)
    print(1)
    blocks_to_parsing_generator = sf.split_list_into_chunks(all_blocks_to_download, QUANTITY_OF_BLOCKS_IN_ITERATION, MAX_ITERATIONS, PROBLEM_BLOCKS_LIST)
    tasks = []
    for blocks_group in blocks_to_parsing_generator:
        start_time = time.time()
        data = await sf.parsing_data(blocks_group)
        print('Данные собраны, сохраняем.')
        min_block_height, max_block_height = sf.get_statistik_data(data)
        save_task = asyncio.create_task(sf.async_save_data_to_parquet(data, min_block_height, max_block_height, DATA_DIRECTORY))
        tasks.append(save_task)
        time_of_circle = sf.print_cicle_info(start_time, min_block_height, max_block_height , len(blocks_group), data, QUANTITY_OF_BLOCKS_IN_ITERATION) # type: ignore
        sf.get_avg_blocks_in_minut(time_of_circle)





# транзакции с большим Transactions_Count - это батчевые 

if __name__ == "__main__":
    asyncio.run(main())
