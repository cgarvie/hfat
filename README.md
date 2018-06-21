# hftbot version 3


Objects:

There are three object types/interfaces which are related in a triangle. Each is connected to the other two directly.

- Exchange
--- gdax
--- poloniex
- Analyzer (allows us to share resources between bots and exchanges)
--- buy wall analyzer brain
--- volume analyzer brain
- Trader
--- penny jumping bot
--- swing trading bot

Trader bots can implement any, or multiple, APIs to place and manage orders. This is entirely separate from "Exchange" type above, which are just a datafeed/input object.

Bots run several co-routines at all times.
- monitor active orders
-- check if stop losses get hit, adjust
-- notice when orders get filled
- look for opportunities (reads from a <-tick channel)
- 

Bots have Task Queues which prevent them from overloading. For example we can only place a limited number of LIMIT orders at a time (depending on balance, risk appetite, etc). So the opportunity-analyzer co-routine only calls placeLimitBuyOrder() via a queue that has a limited capacity. Tasks arent marked as done by placeLimitBuyOrder until we get a response from the GDAX server.

Unlike in v1, where exchanges, analysis, and bots run in a step-by-step sequential order, in v2 everything is always running all of the time. 

Open source the general structure but keep analyzers and traders proprietary.