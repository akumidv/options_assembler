"""alphavar.options — options & futures domain (R0).

Internal shape, by layer then function:
- facade (flat at this root): ``Option`` and its components (``OptionsData``,
  ``OptionsEnrichment``, ``OptionsChain``, ``OptionsAnalytic``, ``ChartClass`` …);
- ``dictionary`` / ``entities`` / ``schemas`` — domain foundation;
- ``lib`` — pure computational logic, by function;
- ``etl`` — I/O orchestration.
"""

from alphavar.options.analytic_class import OptionsAnalytic
from alphavar.options.analytic_price_class import OptionsAnalyticPrice
from alphavar.options.analytic_risk_class import OptionsAnalyticRisk
from alphavar.options.chain_class import OptionsChain
from alphavar.options.chart_class import ChartClass
from alphavar.options.chart_price_class import ChartPriceClass
from alphavar.options.enrichment_class import OptionsEnrichment
from alphavar.options.forecast_class import OptionsForecast
from alphavar.options.option_class import Option
from alphavar.options.option_data_class import OptionsData
from alphavar.options.pricer_class import OptionsPricer
from alphavar.options.validation_class import OptionsValidation

__all__ = [
    "Option",
    "OptionsData",
    "OptionsEnrichment",
    "OptionsChain",
    "OptionsAnalytic",
    "OptionsAnalyticPrice",
    "OptionsAnalyticRisk",
    "OptionsPricer",
    "OptionsForecast",
    "OptionsValidation",
    "ChartClass",
    "ChartPriceClass",
]
