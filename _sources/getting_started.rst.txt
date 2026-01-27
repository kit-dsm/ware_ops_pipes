Getting Started
===============

Understanding  Domain Objects
====================================

Warehouse data is represented through **domain objects**. Understanding these is essential to use the library.

Storage Locations
-----------------

Storage locations define where articles are physically stored in your warehouse.

.. code-block:: python

   from ware_ops_algos.domain_models import Location, StorageLocations, StorageType

   # A single storage location
   location = Location(
       x=1,          # Aisle number
       y=3,          # Position along the aisle
       article_id=1, # Which article is stored here
       amount=10     # Quantity available
   )

   # Collection of all locations in the warehouse
   storage = StorageLocations(
       StorageType.DEDICATED,  # or StorageType.SCATTERED
       locations=[location1, location2, ...]
   )

**Storage Types:**
- ``DEDICATED``: Each article has exactly one location
- ``SCATTERED``: Articles can be stored in multiple locations

Orders
------

Orders represent what customers ordered and need to be picked.

.. code-block:: python

   from ware_ops_algos.domain_models import Order, OrderPosition, Orders, OrderType

   # What was ordered
   order_position = OrderPosition(
       order_number=1,
       article_id=5,
       amount=2
   )

   # Complete order with all positions
   order = Order(
       order_number=1,
       order_positions=[order_position1, order_position2, ...]
   )

   # Collection of all orders
   orders = Orders(
       OrderType.STANDARD,
       orders=[order1, order2, ...]
   )

Resources
---------

Resources represent pickers (humans or robots) who fulfill orders.

.. code-block:: python

   from ware_ops_algos.domain_models import Resource

   picker = Resource(id=1)

Layout (for routing algorithms)
--------------------------------

Some routing algorithms need layout parameters to calculate distances.

.. code-block:: python

   # RatliffRosenthalRouting requires these parameters:
   router = RatliffRosenthalRouting(
       start_node=(0, 0),              # Where picker starts (depot)
       end_node=(4, 0),                # Where picker ends
       n_aisles=6,                     # Number of aisles
       n_pick_locations=15,            # Total pick locations
       dist_aisle=2,                   # Distance between aisles
       dist_pick_locations=1,          # Distance between locations in aisle
       dist_aisle_location=1,          # Distance from aisle to location
       dist_start=1,                   # Distance from depot to first aisle
       dist_end=1,                     # Distance from last aisle to depot
       # ...
   )

Complete Example
================

Now let's put it all together:

.. code-block:: python

   from ware_ops_algos.algorithms import RatliffRosenthalRouting, GreedyPickLocationSelector
   from ware_ops_algos.domain_models import (
       Location, StorageLocations, StorageType,
       Order, OrderPosition, Orders, OrderType,
       Resource
   )

   # 1. Define where articles are stored
   locations_list = [
       Location(x=1, y=3, article_id=1, amount=1),
       Location(x=1, y=8, article_id=2, amount=1),
       Location(x=2, y=5, article_id=3, amount=1),
   ]
   storage_locations = StorageLocations(StorageType.DEDICATED, locations=locations_list)

   # 2. Define what was ordered
   order_positions = [
       OrderPosition(order_number=1, article_id=1, amount=1),
       OrderPosition(order_number=1, article_id=2, amount=1),
       OrderPosition(order_number=1, article_id=3, amount=1),
   ]
   order = Order(order_number=1, order_positions=order_positions)
   orders = Orders(OrderType.STANDARD, orders=[order])

   # 3. Map order positions to physical locations
   #    (This step resolves "I need article 1" to "Pick from location x=1, y=3")
   selector = GreedyPickLocationSelector(orders.orders, storage_locations)
   resolved_orders = selector.select()
   orders.orders = resolved_orders

   # 4. Create pick list for routing
   pick_list = [pos for pos in orders.orders[0].order_positions]

   # 5. Calculate optimal route
   router = RatliffRosenthalRouting(
       start_node=(0, 0),
       end_node=(4, 0),
       closest_node_to_start=(0, 0),
       min_aisle_position=1,
       max_aisle_position=6,
       picker=[Resource(id=1)],
       n_aisles=6,
       n_pick_locations=15,
       dist_aisle=2,
       dist_pick_locations=1,
       dist_aisle_location=1,
       dist_start=1,
       dist_end=1
   )

   solution = router.solve(pick_list)

   print(f"Total distance: {solution.objective_value}")
   print(f"Route: {solution.route}")

