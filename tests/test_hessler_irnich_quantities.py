from ware_ops_algos.domain_models import StorageType
from ware_ops_pipes.data_loaders import HesslerIrnichLoader


def _parsed(*, skus, orders):
    return {
        "header": {
            "TYPE": "Order_batching",
            "NUM_AISLES": "1",
            "NUM_CELLS": "10",
            "DEPOT_AISLE": "0",
            "DEPOT_LOCATION": "bottom",
            "DISTANCE_AISLE_TO_AISLE": "2.4",
            "DISTANCE_CELL_TO_CELL": "1",
            "DISTANCE_TOP_TO_CELL": "1",
            "DISTANCE_BOTTOM_TO_CELL": "1",
            "DISTANCE_TOP_OR_BOTTOM_TO_DEPOT": "0",
            "PICKER_CAPACITY": "10",
        },
        "articles": [{"article_id": 5, "weight": 1}],
        "skus": skus,
        "orders": orders,
    }


def test_dedicated_jobprp_supply_covers_all_order_demand(tmp_path):
    parsed = _parsed(
        skus=[
            {
                "article_id": 5,
                "aisle": 0,
                "cell": 3,
                "quantity": 1,
                "side": "left",
            }
        ],
        orders=[
            [{"article_id": 5, "amount": 2}],
            [{"article_id": 5, "amount": 3}],
        ],
    )

    domain = HesslerIrnichLoader(
        tmp_path, mirror_top_depot=False
    ).build_domain_with_layout(parsed, None)

    assert domain.storage.tpe == StorageType.DEDICATED
    assert domain.storage.locations[0].amount == 5


def test_scattered_storage_keeps_published_location_supplies(tmp_path):
    parsed = _parsed(
        skus=[
            {
                "article_id": 5,
                "aisle": 0,
                "cell": 3,
                "quantity": 1,
                "side": "left",
            },
            {
                "article_id": 5,
                "aisle": 0,
                "cell": 7,
                "quantity": 2,
                "side": "left",
            },
        ],
        orders=[[{"article_id": 5, "amount": 3}]],
    )

    domain = HesslerIrnichLoader(
        tmp_path, mirror_top_depot=False
    ).build_domain_with_layout(parsed, None)

    assert domain.storage.tpe == StorageType.SCATTERED
    assert [location.amount for location in domain.storage.locations] == [1, 2]
