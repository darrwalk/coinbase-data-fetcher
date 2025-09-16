"""Progress bar abstraction for the fetcher."""

from abc import ABC, abstractmethod
from typing import Optional

try:
    from tqdm import tqdm
    HAS_TQDM = True
except ImportError:
    HAS_TQDM = False


class ProgressBar(ABC):
    @abstractmethod
    def update(self, n: int = 1):
        pass
    
    @abstractmethod
    def progress(self, perc: float):
        pass
    
    @abstractmethod
    def text(self, text: str):
        pass
    
    @abstractmethod
    def empty(self):
        pass


class NullProgressBar(ProgressBar):
    def update(self, n: int = 1):
        pass
    
    def progress(self, perc: float):
        pass
    
    def text(self, text: str):
        pass
    
    def empty(self):
        pass


class TqdmProgressBar(ProgressBar):
    def __init__(self, total: int = 100, desc: Optional[str] = None):
        if not HAS_TQDM:
            raise ImportError("tqdm is required for TqdmProgressBar. Install with: pip install tqdm")
        self.pbar = tqdm(total=total, desc=desc, ncols=100, bar_format='{desc}: {percentage:3.0f}%|{bar}| {postfix}')
        self.last_perc = 0
        
    def update(self, n: int = 1):
        self.pbar.update(n)
    
    def progress(self, perc: float):
        delta = int((perc - self.last_perc) * self.pbar.total)
        self.pbar.update(delta)
        self.last_perc = perc
    
    def text(self, text: str):
        self.pbar.set_postfix_str(text)
    
    def empty(self):
        self.pbar.close()