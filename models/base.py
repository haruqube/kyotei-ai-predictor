"""予測モデル抽象基底クラス"""

from abc import ABC, abstractmethod
import pandas as pd


class BasePredictor(ABC):
    """予測モデルの基底クラス"""

    @abstractmethod
    def train(self, X_train: pd.DataFrame, y_train: pd.Series, group_train: list[int]):
        """モデルを学習"""
        pass

    @abstractmethod
    def predict(self, X: pd.DataFrame) -> pd.Series:
        """予測スコアを返す"""
        pass

    @abstractmethod
    def save(self, path: str):
        """モデルを保存"""
        pass

    @abstractmethod
    def load(self, path: str):
        """モデルを読み込み"""
        pass
