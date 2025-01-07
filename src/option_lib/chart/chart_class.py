"""Realisation for option class"""

from plotly.offline import iplot
from option_lib.option_data_class import OptionData
from option_lib.chart.price.chart_price_class import ChartPriceClass


class ChartClass:
    """Base option class that provide possibility to work with option data different way"""
    _data: OptionData
    price: ChartPriceClass

    def __init__(self, data: OptionData):
        self._data = data
        self.price = ChartPriceClass(data)
        self.init()

    def init(self):
        """Init new chart"""
        self.price.empty_figure_data()

    def show(self):
        """Show prepared data"""
        data = []
        data.extend(self.price.figure_data)
        iplot(data)
