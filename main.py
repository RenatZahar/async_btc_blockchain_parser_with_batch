import async_fun as sf
import pandas as pd
import time
import asyncio
from pathlib import Path

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.max_colwidth', None)
pd.set_option('display.precision', 2)
pd.set_option('display.expand_frame_repr', False)

BASE_DIR = Path(__file__).resolve().parent


TEST = 0 
QUANTITY_OF_BLOCKS_IN_ITERATION = 2
MAX_ITERATIONS = 10000 
REQUESTS_QUANTITY = 800
MIN_VALUE_THRESHOLD = 0.01
MAX_LINES_IN_TX_CACHE = 500000
MAX_LINES_IN_HASH_CACHE = 0
START_BLOCK = 740000
BTC_PRICE_DIR = BASE_DIR.parent.parent / 'bitcoin_price' / 'BTCBUSD.csv.gz'
DATA_DIRECTORY = BASE_DIR.parent / 'data'

# если биткоин кор зависает - уменьшаем блоки в чанке
# кэш по хэшам не используется в данной версии

PROBLEM_BLOCKS_LIST = 0 

async def main():
    last_block = sf.get_existing_last_block(DATA_DIRECTORY, START_BLOCK)
    rpc_connection_main = sf.get_rpc_connection()
    all_blocks_to_download = sf.get_list_of_blocks_to_download(last_block, rpc_connection_main)
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

if __name__ == "__main__":
    asyncio.run(main())
