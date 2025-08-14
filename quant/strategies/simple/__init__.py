# Simple strategies package

from .bollinger import BollingerBands
from .roc import RateOfChange
from .random_baseline import RandomBaseline
from .index_strategy import IndexStrategy

__all__ = ['BollingerBands', 'RateOfChange', 'RandomBaseline', 'IndexStrategy']