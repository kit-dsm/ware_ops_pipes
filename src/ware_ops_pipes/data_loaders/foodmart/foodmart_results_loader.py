import pandas as pd
from ware_ops_algos.data_loaders.base_data_loader import DataLoader


class FoodmartResultsLoader(DataLoader):
    def load(self, instance_set: str) -> pd.DataFrame:
        df = pd.DataFrame()
        return df
