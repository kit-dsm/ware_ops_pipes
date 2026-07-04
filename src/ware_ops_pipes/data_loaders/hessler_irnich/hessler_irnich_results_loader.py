from pathlib import Path

import pandas as pd
from ware_ops_algos.data_loaders.base_data_loader import DataLoader


class HesslerIrnichResultsLoader(DataLoader):
    """Loads literature benchmark results"""

    def load(self, instance_set: str | Path) -> pd.DataFrame:
        """Load results for instance set"""
        if "SS" in instance_set:
            df = self._load_csv(instance_set, sep=";")
            if "LB" in df.columns:
                df["LB"] = df["LB"].str.replace(",", ".").astype(float)
            df["demand_helper"] = df.apply(lambda row: "unit" if row["unit demand"] else "varying", axis=1)
            df["filename"] = df.apply(
                lambda
                    row: f"{row['demand_helper']}_F{row['alpha']}_m{row['num aisles']}_C{row['num cells']}_a{row['num articles']}_{row['random seed']}.txt",
                axis=1
            )
            df.rename(columns={"GS MIP cost": "LB"}, inplace=True)
        else:
            df = self._load_csv(instance_set, sep=",")
            df["filename"] = df.apply(
                lambda row: f"unit_F1_m{row['num aisles']}_C{row['num cells']}_a{row['num articles']}_{row['random seed']}",
                axis=1
            )
            df.rename(columns={"Netw MIP cost": "LB"}, inplace=True)
        return df
