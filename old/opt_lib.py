import os

import pandas as pd

OPT_COL_SHOW = ['settlement_price_usd_c', 'settlement_price_c', 'mark_iv_c',
                'symbol', 'expiration', 'strike', # 'underlying_index'
                'mark_iv_p', 'settlement_price_p', 'settlement_price_usd_p']
_opt_add_col_c = ['money_c', 'Intrinsic Value C', 'Time Value C']
_opt_add_col_p = ['Intrinsic Value P', 'Time Value P', 'money_p']
OPT_COL_VAL_SHOW = _opt_add_col_c + OPT_COL_SHOW + _opt_add_col_p


EXAMPLE_DIR = os.path.normpath(os.path.join(os.path.dirname(__file__), '../../data/.data_example/deribit')) # 2023-08-27

def load_opt_table():
    opt_table_df = pd.read_parquet(f'{EXAMPLE_DIR}/options_table.parquet')
    opt_table_df['strike'] = opt_table_df['strike'].astype('int') # TODO remove
    return opt_table_df


def get_strike_step(symb_opt: pd.DataFrame):
    strike_step = symb_opt['strike'].iloc[int(len(symb_opt) / 2) + 1] - symb_opt['strike'].iloc[int(len(symb_opt) / 2)]
    print('strike_step', strike_step)
    return strike_step


def get_cur_price(symb_opt: pd.DataFrame):
    cur_symb_price = symb_opt['underlying_price'].iloc[0]
    print('cur_symb_price:', cur_symb_price)
    return cur_symb_price


def calc_values(symb_opt: pd.DataFrame):
    symb_opt = symb_opt.copy()

    cur_symb_price = get_cur_price(symb_opt)
    strike_step = get_strike_step(symb_opt)

    symb_opt['Intrinsic Value C'] = symb_opt.apply(lambda x: None if x['settlement_price_usd_c'] is None else \
                                    (cur_symb_price - x['strike'] if cur_symb_price - x['strike'] > 0 else 0), axis=1)
    symb_opt['Time Value C'] = symb_opt.apply(lambda x: x['settlement_price_usd_c'] - x['Intrinsic Value C'] if \
                                                        x['settlement_price_usd_c'] is not None else 0, axis=1)
    symb_opt['money_c'] = symb_opt.apply(lambda x: 'ATM' if abs(x['strike'] - cur_symb_price) < strike_step else \
                                                   ('ITM' if x['Intrinsic Value C'] > 0 else 'OTM'), axis=1)
    symb_opt['Intrinsic Value P'] = symb_opt.apply(lambda x: None if x['settlement_price_usd_p'] is None else (
                                    x['strike'] - cur_symb_price if x['strike'] - cur_symb_price > 0 else 0), axis=1)
    symb_opt['Time Value P'] = symb_opt.apply(lambda x: x['settlement_price_usd_p'] - x['Intrinsic Value P'] if \
                                                        x['settlement_price_usd_p'] is not None else 0, axis=1)
    symb_opt['money_p'] = symb_opt.apply(lambda x: 'ATM' if abs(x['strike'] - cur_symb_price) < strike_step else \
                                                   ('ITM' if x['Intrinsic Value P'] > 0 else 'OTM'), axis=1)
    return symb_opt


def get_symbol_expirations(opt_df: pd.DataFrame, symbol: str):
    symb_expirations = list(opt_df[opt_df['symbol'] == symbol]['expiration'].unique())
    print(f'{symbol} expirations:', symb_expirations)
    return symb_expirations


def get_symbol_option_order_table(opt_table_df: pd.DataFrame, symbol: str = 'BTC', expiration_idx: int = 7):
    # instruments = list(options_table_df['underlying_index'].unique())
    # print(instruments)
    # ticker_options = list(filter(lambda x: x.startswith(f'{ticker}-'), instruments))
    # df = options_table_df[options_table_df['underlying_index'] == ticker_options[-1]].sort_index(axis=1)
    # print(df)

    print('symbol:', symbol)
    symb_expiration = get_symbol_expirations(opt_table_df, symbol)
    print(f'{symbol} expiration:', symb_expiration)
    cur_timestamps = opt_table_df[(opt_table_df['expiration'] == symb_expiration[0])&(opt_table_df['timestamp_c'].notnull())]['timestamp_c'].unique()[0]
    print('cur_timestamps:', cur_timestamps)
    symb_expiration = symb_expiration[expiration_idx]
    print('symb_expiration:', symb_expiration)
    # syn_symbol = f'SYN.{symbol}-{symb_expiration}'
    # print('syn_symbol', syn_symbol)
    # cur_syn_symb_price = options_table_df[options_table_df['underlying_index'] == syn_symbol]
    # print('cur_syn_symb_price', cur_syn_symb_price[OPT_COL_SHOW])
    symb_opt = opt_table_df[(opt_table_df['symbol'] == symbol) & (opt_table_df['expiration'] == symb_expiration)].sort_values(by='strike')
    return symb_opt


def get_opt_table(symbol: str = 'BTC'):
    options_table_df = load_opt_table()
    # print(options_table_df.columns)
    print('symbols', list(options_table_df['symbol'].unique()))
    instruments = list(options_table_df['underlying_index'].unique())
    print('instruments:', instruments)
    symb_opt = get_symbol_option_order_table(options_table_df, symbol)
    # print(symb_opt[OPT_COL_SHOW])
    symb_opt_ext = calc_values(symb_opt)
    print(symb_opt_ext[OPT_COL_VAL_SHOW])
    return symb_opt_ext

