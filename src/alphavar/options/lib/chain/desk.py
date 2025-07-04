"""
Prepare desk for option chain
"""

from pandera.typing import DataFrame

from alphavar.options.dictionary import OptionsTerm, OptionsType
from alphavar.options.schemas import DeskSchema


def convert_chain_to_desk(
    df_chain, option_columns: list | None = None, future_columns: list | None = None
) -> DataFrame[DeskSchema]:
    """Prepare option desk from options or option_chain"""
    if option_columns is None:
        option_columns = [OptionsTerm.TIMESTAMP, OptionsTerm.EXPIRATION_DATE, OptionsTerm.STRIKE, OptionsTerm.PRICE]
    append_col = [
        col
        for col in [OptionsTerm.TIMESTAMP, OptionsTerm.EXPIRATION_DATE, OptionsTerm.STRIKE]
        if col not in option_columns
    ]
    if len(append_col):
        option_columns += append_col
    option_columns = [col for col in option_columns if col in df_chain.columns]
    if future_columns is None:
        future_columns = [
            col
            for col in [OptionsTerm.UNDERLYING_PRICE, OptionsTerm.UNDERLYING_EXPIRATION_DATE]
            if col in df_chain.columns
        ]
    future_columns = [col for col in future_columns if col in df_chain.columns]

    df_hist_desk = (
        df_chain[df_chain[OptionsTerm.OPTION_RIGHT] == OptionsType.CALL.value][option_columns]
        .merge(
            df_chain[df_chain[OptionsTerm.OPTION_RIGHT] == OptionsType.PUT.value][option_columns],
            on=[OptionsTerm.TIMESTAMP, OptionsTerm.EXPIRATION_DATE, OptionsTerm.STRIKE],
            suffixes=["_call", "_put"],
            how="outer",
        )
        .sort_values(by=OptionsTerm.STRIKE)
    )
    if future_columns:
        df_fut = df_chain[[OptionsTerm.TIMESTAMP, OptionsTerm.EXPIRATION_DATE] + future_columns].drop_duplicates()
        df_hist_desk = df_hist_desk.merge(df_fut, on=[OptionsTerm.TIMESTAMP, OptionsTerm.EXPIRATION_DATE], how="left")
    return df_hist_desk
