

# Some resources with interesting ideas to check them
* Option scanner of [Option Samurai](https://samurai.froged.help/docs/en/52619420-option-samurai-overview)
*


## Metrics
* implement caclulation of Rate Time value and Volatility Value, idea from Checkulaev
  Zagadki i tayny optionnoy torgovlyu, page 36. Price of Option = Intrincic value + Time Value + Volatility Value.
  But in this case Time value - is mostly value of cost of money and can be mention as Rate Time Value. But then
  Volatility Value will be function of time, not only volatility. But still measure of risk. Another way -
  calc time decreasing by history (for this expiration or for few near) fitted curve (when iv identical for
  example or with some difference correction) and get Volatility Value as difference. Next way use for etalon base active volatility S&P.


Data improvements
* arbitrage removal, for example https://github.com/vicaws/arbitragerepair/tree/master/arbitragerepair
* iv fitting
* data missed strikes premium forward and backwards fill
