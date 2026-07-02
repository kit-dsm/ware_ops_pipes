from __future__ import annotations

import ware_ops_algos.algorithms as algo_module


_GENERATED_CLASSES = {}


def resolve_card_value(value):
    if isinstance(value, dict):
        if "$class" in value:
            return getattr(algo_module, value["$class"])
        return {key: resolve_card_value(val) for key, val in value.items()}

    if isinstance(value, list):
        return [resolve_card_value(item) for item in value]

    return value


def make_configured_component(card, base_cls):
    key = (base_cls.__name__, card.algo_name)

    if key in _GENERATED_CLASSES:
        return _GENERATED_CLASSES[key]

    cls = type(
        card.algo_name,
        (base_cls,),
        {
            "abstract": False,
            "algorithm_card": card,
            "__module__": base_cls.__module__,
            "__qualname__": card.algo_name,
        },
    )

    _GENERATED_CLASSES[key] = cls
    return cls


class CardConfiguredComponent:
    algorithm_card = None

    def implementation_config(self) -> dict:
        return self.algorithm_card.implementation or {}

    def raw_parameters(self) -> dict:
        return self.implementation_config().get("parameters", {})

    def configured_parameters(self) -> dict:
        return resolve_card_value(self.raw_parameters())

    def config_fingerprint_payload(self) -> dict:
        return {
            "algo_name": self.algorithm_card.algo_name,
            "implementation": self.implementation_config(),
            "runtime": {
                "time_limit_sec": self.pipeline_params.time_limit_sec,
                "gen_tour": self.pipeline_params.gen_tour,
            },
        }