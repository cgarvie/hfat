package main

import (
	"bitbot/bitmex/bitmex"
	bmwebsocket "bitbot/bitmex/bitmex/websocketApi"
	//"encoding/json"
	"fmt"
	ws "github.com/gorilla/websocket"
	gdax "github.com/preichenberger/go-gdax"
	//"math/big"
	//"math"
	"log"
	"math/rand"
	"sort"
	"strconv"
	"time"
	//	"os"
	//"database/sql"
	"encoding/json"
	"github.com/davecgh/go-spew/spew"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"html/template"
	"net/http"
	"os"
	"os/signal"
	"sync"
)

type Configuration struct {
	Database map[string]string
	GDAX     map[string]string
}


func removeBookEntry(s []gdax.BookEntry, i int) []gdax.BookEntry {
	s[len(s)-1], s[i] = s[i], s[len(s)-1]
	return s[:len(s)-1]
}

func removePaperTrade(s []paperTrade, i int) []paperTrade {
	s[len(s)-1], s[i] = s[i], s[len(s)-1]
	return s[:len(s)-1]
}

func (book *OrderBookEngine) reset_book() { // reset_book(client *gdax.Client)
	book.Book, _ = book.Client.GetBook(book.Pair, 3)
}

var schema = `
CREATE TABLE trades (
	botid integer,
    side text,
    size decimal,
    price decimal,
    orderid text,
    createdat timestamp,
    executedat timestamp,
    type text,
    correspondingbuyorderid text
),

CREATE TABLE bots (
	id integer,
    name text
);`

/**
bots implement the Trading functions (create, cancel, execute)
and maintain their own Trades[] array.

In the loop, we add logic for each bot individually.
**/

type bot struct {
	Id                 int    `db:"id"`
	Name               string `db:"name"`
	ETH_wallet_balance int    // This is measured in 0.01 ETHs
	//paperTrades        []paperTrade
	//lastBuyOrder       paperTrade
	//lastSellOrder      paperTrade
	openOrders    map[string][]paperTrade
	RealMoneyMode bool
}

type paperTrade struct {
	BotId                   int       `db:"bot_id"`
	Side                    string    `db:"side"`
	Size                    float64   `db:"size"`
	Price                   float64   `db:"price"`
	OrderId                 string    `db:"order_id"`
	CorrespondingBuyOrderId string    `db:"corresponding_buy_order_id"` // if Type="sell" then probably has this
	Type                    string    `db:"type"`
	CreatedAt               time.Time `db:"created_at"`
	ExecutedAt              time.Time `db:"executed_at"`
	//Active      bool
	//CancelledAt time.Time
}

func (b *PennyBot) init() {
	b.channel = make(chan bool)
	//
	b.openOrders = make(map[string][]paperTrade)
	b.openOrders["buy"] = []paperTrade{}
	b.openOrders["sell"] = []paperTrade{}
	//
	b.NewlyCancelledOrders = make(chan string)
	b.NewlyExecutedOrders = make(chan string)
	//
	b.AvailableBalance = make(map[string]float64)
}

func ReducePrecision(x float64) float64 {
	// lol
	tmp := fmt.Sprintf("%.2f", x)
	r, _ := strconv.ParseFloat(tmp, 2)
	return r
}

func CreateTradeFromGDAXOrder(o *gdax.Order) *paperTrade {
	// We pass a pointer to the trade so that if it
	// is updated by the back-end we can read the new attributes.
	p := paperTrade{
		Type:       o.Type,
		Side:       o.Side,
		Size:       o.Size,
		Price:      o.Price,
		OrderId:    o.Id,
		CreatedAt:  time.Time(o.CreatedAt),
		ExecutedAt: time.Time{},
	}
	return &p
}

func (b *PennyBot) UpdateBalances() {
	for true {
		select {
		case <-b.Book.Kill_All_Existing_Goroutines:
			return
		default:
			time.Sleep(5 * time.Second)

			accounts, err := b.Book.Client.GetAccounts()
			if err != nil {
				println(err.Error())
			}

			for _, a := range accounts {
				b.mutex.Lock()
				b.AvailableBalance[a.Currency] = a.Balance
				b.mutex.Unlock()
			}
		}
	}
}

func (b *PennyBot) UpdateGDAXOrders() {

	/*
		re-architect so these polls as few times as possible yet power multiple bots.
	*/

	for true {
		select {
		case <-b.Book.Kill_All_Existing_Goroutines:
			return
		default:
			time.Sleep(2 * time.Second)

			var orders []gdax.Order
			cursor := b.Book.Client.ListOrders(gdax.ListOrdersParams{Status: "all"})

			for cursor.HasMore {
				if err := cursor.NextPage(&orders); err != nil {
					println(err.Error())
					return
				}
				b.mutex.Lock()
				for _, g := range orders {
					if g.Status == "done" {
						for side, oOrders := range b.openOrders {
							for i, o := range oOrders {
								if g.Id == o.OrderId {
									log.Println("Processing Done GDAX order", o.OrderId)
									b.openOrders[side][i].ExecutedAt = time.Now() // Roughly!
									b.NewlyExecutedOrders <- o.OrderId
								}
							}
						}
					}
				}
				b.mutex.Unlock()
			}
		}
	}
}

func (b *PennyBot) cancelAllGDAXOrders() error {
	_, err := b.Book.Client.Request("DELETE", "/orders", nil, nil)
	return err
}

func (b *PennyBot) createPaperTrade(side string, size float64, price float64, optionalCorrespondingBuyOrderId string) *paperTrade {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	price = ReducePrecision(price)
	log.Println("Creating", side, "with size", size, "@ price", price)
	if b.RealMoneyMode == false {
		newTrade := paperTrade{BotId: b.Id,
			Type:       "limit", //default
			Side:       side,
			Size:       size,
			Price:      price,
			OrderId:    time.Now().Format("2006-01-02_15-04-05_") + fmt.Sprintf("%x", rand.Intn(999999999)),
			CreatedAt:  time.Now(),
			ExecutedAt: time.Time{}}
		if optionalCorrespondingBuyOrderId != "" {
			newTrade.CorrespondingBuyOrderId = optionalCorrespondingBuyOrderId
		}
		b.openOrders[side] = append(b.openOrders[side], newTrade)
		return &newTrade
	} else if b.RealMoneyMode == true {

		// Note: A GDAX Limit order is no guarantee that it will execute as a limit order. If the mid-price/spread
		// has shifted such that you are placing into the market price, it will be executed as a Market order
		// and you will pay a fee.
		order := gdax.Order{
			Type:      "limit",
			Price:     price,
			Size:      size,
			Side:      side,
			ProductId: b.Book.Pair,
		}

		savedOrder, err := b.Book.Client.CreateOrder(&order)
		if err != nil {
			println(err.Error())
			log.Println(err.Error())
			return nil
		} else {
			t := CreateTradeFromGDAXOrder(&savedOrder)
			log.Println("Created trade logged with id", t.OrderId, "with size", t.Size, "@ price", t.Price)
			b.openOrders[side] = append(b.openOrders[side], *t)
			return t
		}
	}
	return nil
}

func (b *PennyBot) executePaperTrade(id string) bool {
	println("executing trade")
	b.mutex.Lock()
	defer b.mutex.Unlock()

	for side, trades := range b.openOrders {
		for index, trade := range trades {
			if trade.OrderId == id {
				trade.ExecutedAt = time.Now()
				_, err := b.db.NamedExec(`INSERT INTO trades 
					(botid, side, size, price, orderid, createdat, executedat, type, correspondingbuyorderid) 
					VALUES (:bot_id,:side,:size,:price,:order_id,:created_at,:executed_at,:type,:corresponding_buy_order_id)`,
					trade)
				if err != nil {
					fmt.Println(err)
					panic(err)
				}
				b.openOrders[side] = removePaperTrade(b.openOrders[side], index)

				log.Println("EXECUTED!", trade.Side, "trade", trade.OrderId, "at price", trade.Price, "of size", trade.Size)

				if side == "buy" {
					b.ETH_wallet_balance += int(trade.Size * float64(100))
				} else if side == "sell" {
					b.ETH_wallet_balance -= int(trade.Size * float64(100))
					// Mark corresponding Fill as no longer Outstanding
					for i, f := range b.Fills {
						if f.CorrespondingSellOrderId == trade.OrderId {
							b.Fills[i].Outstanding = false
						}
					}
				}
				return true
			}
		}
	}
	return true
}

func (b *PennyBot) cancelGDAXOrder(id string) {
	log.Println("Cancelling GDAX order", id)
	err := b.Book.Client.CancelOrder(id)
	if err == nil {
		log.Println("err == nil")
		b.NewlyCancelledOrders <- id // This is blocking. Shouldnt it be non-blocking?
	} else if err.Error() == "Order already done" {
		println(err.Error())
		log.Println("ERROR WHEN CANCELLING ORDER", err.Error())
		// We will find out it executed from our Polling of UpdateGDAXOrders()
	} else if err.Error() == "order not found" {
		// It executed before we could cancel it.
		log.Println("order not found")
		//b.NewlyExecutedOrders <- id
		// We will find out it executed from our Polling of UpdateGDAXOrders()
	} else if err.Error() == "Insufficient funds" {
		println(err.Error())
		log.Println("ERROR WHEN CANCELLING ORDER", err.Error())
	} else {
		//panic(err.Error()) // Panic for now, until bugs are all worked out.
		log.Println("ERROR WHEN CANCELLING ORDER", err.Error())
	}
	return
}

func (b *PennyBot) cancelPaperTrade(id string) bool {
	log.Println("Cancelling trade", id)
	b.mutex.Lock()
	defer b.mutex.Unlock()

	if b.RealMoneyMode == true {
		go b.cancelGDAXOrder(id)
		// we can't safely remove it from openOrders because
		// it may execute, and we need the information for creating
		// a corresponding SELL and Stop Loss
	} else {
		for side, trades := range b.openOrders {
			for index, trade := range trades {
				if trade.OrderId == id {
					b.openOrders[side] = removePaperTrade(b.openOrders[side], index)
					return true
				}
			}
		}
	}
	return false
}

func float64InSlice(a float64, list []float64) bool {
	for _, b := range list {
		if b == a {
			return true
		}
	}
	return false
}

func mapHasKeyInRange(floatMap map[float64]float64, bound_lower float64, bound_upper float64) bool {
	for k, _ := range floatMap {
		if bound_lower <= k {
			if k <= bound_upper {
				return true
			}
		}
	}
	return false
}

/*
A PennyBot instance has a Bot and a pointer to an AdvancedOrderBook
*/
type PennyBot struct {
	bot
	db                   *sqlx.DB
	mutex                sync.Mutex
	channel              chan bool
	goRoutineQuitChannel *chan bool
	//DataLayers []*DataLayer
	Book             *OrderBookEngine
	BuyWallDataLayer *BuyWallDataLayer
	Fills            []Fill
	//
	existing_buy_walls []float64
	//
	limit_sell_order_position float64
	limit_buy_order_position  float64
	// we'd rather lose a few pennies than only sell 50% of the time
	sell_safety_factor float64
	buy_safety_factor  float64
	//
	NewlyExecutedOrders  chan string
	NewlyCancelledOrders chan string
	// Balances/wallets
	AvailableBalance map[string]float64
	//
	//NeverMoveSellOrderFurtherAway bool
	//
	// for the web interface
	cached_stream string
}

type Fill struct {
	CorrespondingBuyOrderId  string // Should change to BuyOrderId
	CorrespondingSellOrderId string // Currently only used by cleverUpdate.
	Size                     float64
	StopLossPrice            float64
	Outstanding              bool // Does this buy still not yet have a corresponding sell fill?
}

type BuyWallDataLayer struct {
	Book              *OrderBookEngine
	channel           chan int
	recievers         []*PennyBot
	mutex             sync.Mutex
	bids              map[float64]float64
	asks              map[float64]float64
	bids_cumulative   map[float64]float64
	asks_cumulative   map[float64]float64
	experimental      map[float64]float64
	experimental_best float64
	buy_walls         map[float64]float64
	buy_walls_keys    []float64 // This is a list of buy_wall keys, in order of descending buy_wall[key] value.
	//sell_position_given_buy_position map[float64]float64
	asks_keys                    []float64
	bids_keys                    []float64
	largest_buy_wall_depth       float64
	largest_buy_wall_position    float64
	largest_buy_wall_volume      float64 // This should really be called largest_buy_wall_power
	minimum_buy_wall_multiple    float64 // as we are no longer using 'volume' literally
	rolling_window_width         float64
	distance_strength_multiplier float64
	average_buy_size             float64
}

func NewBuyWallDataLayer(book *OrderBookEngine) BuyWallDataLayer {
	dl := BuyWallDataLayer{Book: book}
	dl.channel = make(chan int) 
	//
	dl.largest_buy_wall_depth = 0
	dl.largest_buy_wall_position = 0
	dl.largest_buy_wall_volume = 0
	//dl.minimum_buy_wall_multiple = 50
	dl.rolling_window_width = 0.20 // 10 cents
	dl.distance_strength_multiplier = 5
	// The larger this number, the less a buy wall's distance from the spread
	// dampens its power. Lower number = Closer buy walls will be preferred.
	dl.average_buy_size = 0
	//
	return dl
}

type DataLayer interface {
	update()
}

type OrderBookEngine struct {
	Configuration                *Configuration
	Client                       *gdax.Client
	Book                         gdax.Book
	updates                      chan *gdax.Message
	DataLayers                   []*BuyWallDataLayer
	wsConn                       *ws.Conn
	Pair                         string
	Kill_All_Existing_Goroutines chan bool
	//Bids        []gdax.BookEntry //map[float64]float64
	//Asks        []gdax.BookEntry //map[float64]float64
	highest_bid float64
	lowest_bid  float64
	highest_ask float64
	lowest_ask  float64
	spread      float64
}

func (self *OrderBookEngine) runGDAX() {

	for true {

		self.StartGDAXClient()

		message := gdax.Message{}

		for true {

			if err := self.wsConn.ReadJSON(&message); err != nil {
				// That's too bad!
				println(err.Error())
				break
			}

			if message.Sequence <= int64(self.Book.Sequence) {
				continue
			} else if message.Sequence > int64(self.Book.Sequence+1) {
				fmt.Printf("ERROR: Sequence gap. Re-grabbing current snapshot. %v vs %v\n", message.Sequence, self.Book.Sequence)
				self.reset_book()
				continue
			} else if message.Sequence == int64(self.Book.Sequence+1) {
				self.Book.Sequence += 1
			}

			if message.Type == "open" {
				order := gdax.BookEntry{message.Price, message.Size, 1, message.OrderId}
				if message.Side == "buy" {
					self.Book.Bids = append(self.Book.Bids, order)
				} else if message.Side == "sell" {
					self.Book.Asks = append(self.Book.Asks, order)
				}
			} else if message.Type == "done" {
				//if message.Price > 0 { // is this even needed?
				// RemainingSize lets us see spoofed orders!
				if message.Side == "buy" {
					for index, bid := range self.Book.Bids {
						if bid.OrderId == message.OrderId {
							self.Book.Bids = removeBookEntry(self.Book.Bids, index)
							break
						}
					}
				} else if message.Side == "sell" {
					for index, ask := range self.Book.Asks {
						if ask.OrderId == message.OrderId {
							self.Book.Asks = removeBookEntry(self.Book.Asks, index)
							break
						}
					}
				}
			} else if message.Type == "match" {
				if message.Side == "buy" {
					for index, bid := range self.Book.Bids {
						if message.MakerOrderId == bid.OrderId {
							if message.Size >= bid.Size {
								self.Book.Bids = removeBookEntry(self.Book.Bids, index)
							} else {
								self.Book.Bids[index].Size -= message.Size
							}
							break
						}
					}
				} else if message.Side == "sell" {
					for index, ask := range self.Book.Asks {
						if message.MakerOrderId == ask.OrderId {
							if message.Size >= ask.Size {
								self.Book.Asks = removeBookEntry(self.Book.Asks, index)
							} else {
								self.Book.Asks[index].Size -= message.Size
							}
							break
						}
					}
				}
			} else if message.Type == "change" {
				if message.Price > 0 { // > 0 means it's for a limit order.
					if message.Side == "buy" {
						for index, bid := range self.Book.Bids {
							if message.MakerOrderId == bid.OrderId {
								fmt.Printf("Change(d) old size %.2f to new size %.2f", self.Book.Bids[index].Size, message.NewSize)
								self.Book.Bids[index].Size = message.NewSize
								self.Book.Bids[index].Price = message.Price
								break
							}
						}
					} else if message.Side == "sell" {
						for index, ask := range self.Book.Asks {
							if message.MakerOrderId == ask.OrderId {
								fmt.Printf("Change(d) old size %.2f to new size %.2f", self.Book.Asks[index].Size, message.NewSize)
								self.Book.Asks[index].Size = message.NewSize
								self.Book.Asks[index].Price = message.Price
								break
							}
						}
					}
				}
			}

			// We need to be clever with this bit or we end up with having highest_bid=0 and/or lowest_bid=0 for a milisecond every once in a while.
			var highest_bid float64 = 0
			var lowest_bid float64 = 999999
			for _, bid := range self.Book.Bids {
				if bid.Price > highest_bid {
					highest_bid = bid.Price
				}
				if bid.Price < lowest_bid {
					lowest_bid = bid.Price
				}
			}
			self.highest_bid = highest_bid
			self.lowest_bid = lowest_bid

			var highest_ask float64 = 0
			var lowest_ask float64 = 9999999999
			for _, ask := range self.Book.Asks {
				if ask.Price < lowest_ask {
					lowest_ask = ask.Price
				}
				if ask.Price > highest_ask {
					highest_ask = ask.Price
				}
			}
			self.highest_ask = highest_ask
			self.lowest_ask = lowest_ask

			self.spread = self.lowest_ask - self.highest_bid

			for i, _ := range self.DataLayers {
				select {
				case self.DataLayers[i].channel <- 1:
					// non blocking send
				default:
					// This case occurs whenever BuyWallDataLayer is still procesing an old message, because the channel has no buffer.
					// This is acceptable behavior because it will update in a milisecond on the next message anyway...
				}
			}

		}

		self.KillGDAXClient()

	}

}

func (self *BuyWallDataLayer) run() {

	/*
		Another option would be to check for new messages in a buffered channel.
		If messages exist, read until none. Then process.
		If messages do not exist, wait for a message.
	*/

	for _ = range self.channel {

		self.mutex.Lock()

		book := self.Book

		range_limit := make(map[string]float64)
		range_limit["bid"] = 2.5
		range_limit["ask"] = 10.0

		self.asks = make(map[float64]float64)
		for _, ask := range book.Book.Asks {
			if ask.Price > book.lowest_ask+range_limit["ask"] {
				continue
			} else {
				self.asks[ask.Price] += ask.Size
			}
		}

		self.bids = make(map[float64]float64)
		for _, bid := range book.Book.Bids {
			if bid.Price < book.highest_bid-range_limit["bid"] {
				continue
			} else {
				self.bids[bid.Price] += bid.Size
			}
		}

		self.asks_keys = make([]float64, 0, len(self.asks))
		for position, _ := range self.asks {
			self.asks_keys = append(self.asks_keys, position)
		}
		self.bids_keys = make([]float64, 0, len(self.bids))
		for position, _ := range self.bids {
			self.bids_keys = append(self.bids_keys, position)
		}

		sort.Float64s(self.asks_keys) // Asks are sorted in increasing order
		sort.Float64s(self.bids_keys) // Bids are sorted in increasing order
		// THE ABOVE LINE MUST PRECEED THE FOLLOWING LINE. VERY IMPORTANT.
		sort.Sort(sort.Reverse(sort.Float64Slice(self.bids_keys))) // Bids are sorted in decreasing order

		total := 0.0
		self.bids_cumulative = make(map[float64]float64)
		for _, position := range self.bids_keys {
			total += self.bids[position]
			self.bids_cumulative[position] = total
		}
		total = 0.0
		self.asks_cumulative = make(map[float64]float64)
		for _, position := range self.asks_keys {
			total += self.asks[position]
			self.asks_cumulative[position] = total
		}

		sort.Float64s(self.bids_keys) // Bids are sorted in increasing order

		/*

			We should consider storing and using median_buy_size instead of avg

		*/

		// Reset for this iteration
		self.largest_buy_wall_depth = 0
		self.largest_buy_wall_position = 0
		self.largest_buy_wall_volume = 0
		self.buy_walls = make(map[float64]float64)
		self.buy_walls_keys = []float64{}

		rolling_window := make(map[float64]float64)

		total = 0.0
		for _, position := range self.bids_keys {
			// don't actually start doing anything until within range.
			if position < self.Book.highest_bid-range_limit["bid"] {
				continue
			}
			// add current position and value to rolling_window
			rolling_window[position] = self.bids[position]
			var sum float64 = 0
			for p, v := range rolling_window {
				if p < position-self.rolling_window_width {
					// remove any values in rolling_window that are now out-of-bounds.
					delete(rolling_window, p)
				} else {
					// get the sum of the rolling_window contents at this position.
					sum += v
				}
			}
			// two rolling_windows with the same volume are equal even
			// if one has more active positions... sort of.
			// alternatively you could divide by float64(len(rolling_window))
			// and then /d
			//d := self.distance_strength_multiplier + (book.highest_bid - position)
			power := (sum / float64(self.rolling_window_width*100)) // / d
			self.buy_walls[position] = power
			self.buy_walls_keys = append(self.buy_walls_keys, position)
			if power > self.largest_buy_wall_volume {
				// We have a new biggest buy wall!
				self.largest_buy_wall_position = position
				self.largest_buy_wall_volume = power
				//largest_buy_wall_height = sum / float64(len(rolling_window))
				//println("rolling window length is", len(rolling_window))
			}
			total += self.bids[position]
		}
		//average_buy_size = total / float64(len(bids_keys))
		self.average_buy_size = total / (range_limit["bid"] * 100) // 1 cent = 1 possible position
		// now sort in descending order to calc cumulative depth
		sort.Sort(sort.Reverse(sort.Float64Slice(self.bids_keys)))
		self.largest_buy_wall_depth = 0
		for _, position := range self.bids_keys {
			self.largest_buy_wall_depth += self.bids[position]
			if position < self.largest_buy_wall_position-self.rolling_window_width {
				break
			}
		}

		/**
		Experimental
		**/

		// Reset
		self.experimental = make(map[float64]float64)
		//self.sell_position_given_buy_position = make(map[float64]float64)
		for _, bid_x := range self.bids_keys { // iterate through bids in increasing order
			depth := self.bids_cumulative[bid_x]
			var ask_x float64 = 0.0
			for _, x := range self.asks_keys { // iterate through asks in increasing order
				if self.asks_cumulative[x] >= depth {
					//println("found one", x, "=", self.asks_cumulative[x])
					ask_x = x
					break
				}
			}
			if ask_x != 0.0 { // Important!
				// Why is the above important? Elaborate! It doesn't look useful at all...
				// It looks like a buy position is EXTRA profitable if no sell position
				// matches the cumulative depth of the buy position...
				// I guess it helps prevent bugs. (?)
				// we use the highest_bid for the ask and not the lowest_ask
				// because sometimes ask_x == lowest_ass
				if ask_x-self.Book.highest_bid > self.Book.lowest_ask-bid_x {
					//fmt.Printf("adding %.4f %.4f %.4f\n", ask_x, bid_x, (ask_x-self.Book.highest_bid)/(self.Book.lowest_ask-bid_x))
					self.experimental[bid_x] = (ask_x - self.Book.highest_bid) / (self.Book.lowest_ask - bid_x)
					//self.sell_position_given_buy_position[bid_x] = ask_x
				}
			}
		}

		var best_score float64 = 0
		// Reset
		self.experimental_best = 0
		for k, v := range self.experimental {
			//println(x, self.experimental[x])
			if v > best_score {
				best_score = v
				self.experimental_best = k
			}
		}

		for i, _ := range self.recievers {
			select {
			case self.recievers[i].channel <- true:
				// non blocking send
			default:
				// This case occurs whenever the reciever/bot is still procesing an old message, because the channel has no buffer.
				// This is acceptable behavior because it will update in a milisecond on the next message anyway...
			}
		}

		self.mutex.Unlock()

	}

}

func (self *PennyBot) cleverUpdate() {

	/*

		This bot uses a clever idea:

		We should be looking for the buy_position (a) for which the
		ask_position with an equal cumulative depth
		is furthest from the spread
		when measured as a multiple of the
		buy_position (a)'s distance from the spread


		Also:

		At each step we open a buy order at each position which is likely to be profitable.
		At each step we also delete all previously-created buy orders which are no longer likely to be profitable.
		Every time a buy is filled, the following is created:
			-	a stop-loss
			-	a sell order at the corresponding position.
		If a stop loss is ever hit, its fill order becomes a "Dynamic Market" order.


	*/

	////
	// Step (1) Executions
	// Step (2) Stop Losses
	// Step (3) Check Existing Buy Orders
	// Step (4) New Buy Orders
	////

	////
	// (1) Check if any of our paper trades should execute
	////

ExecutingTrades:

	if self.RealMoneyMode == false {
		for side, trades := range self.openOrders {
			for _, trade := range trades {
				if side == "buy" {
					//if trade.Price >= self.Book.lowest_ask-float64(0.01) {
					if trade.Price >= self.Book.lowest_ask {

						// execute
						self.executePaperTrade(trade.OrderId)

						// create corresponding sell order
						var ideal_price float64
						depth_at_fill_wall := self.BuyWallDataLayer.bids_cumulative[trade.Price-self.BuyWallDataLayer.rolling_window_width]
						for _, x := range self.BuyWallDataLayer.asks_keys { // iterate through asks in increasing order
							if self.BuyWallDataLayer.asks_cumulative[x] >= depth_at_fill_wall*self.sell_safety_factor {
								ideal_price = x
								break
							}
						}
						o := self.createPaperTrade("sell", 0.01, ideal_price, trade.OrderId)
						if o == nil {
							log.Println("WARNING: we failed to create a corresponding sell order for our executed buy order because of an error.")
							goto ExecutingTrades
						}
						//o.CorrespondingBuyOrderId = trade.OrderId
						// THIS DOES NOT DO ANYTHING ^

						// add new outstanding fill to Fills
						f := Fill{
							CorrespondingBuyOrderId:  trade.OrderId,
							CorrespondingSellOrderId: o.OrderId,
							Size:          trade.Size,
							StopLossPrice: trade.Price - self.BuyWallDataLayer.rolling_window_width,
							Outstanding:   true,
						}
						self.Fills = append(self.Fills, f)

						goto ExecutingTrades
					}
				} else if side == "sell" {
					//if trade.Price <= self.Book.highest_bid+float64(0.01) {
					if trade.Price <= self.Book.highest_bid {

						// execute
						self.executePaperTrade(trade.OrderId)

						goto ExecutingTrades

					}
				}
			}
		}
	} else if self.RealMoneyMode == true {
		select {
		case id := <-self.NewlyExecutedOrders:
			log.Println("Processing Newly Executed Order", id)
			// We have an updated Order!
			for _, openOrders := range self.openOrders {
				for _, trade := range openOrders {
					if trade.OrderId == id {
						// It was executed but not yet processed/removed!
						if trade.Side == "buy" {

							// execute
							self.executePaperTrade(trade.OrderId)

							// create corresponding sell order
							var ideal_price float64
							depth_at_fill_wall := self.BuyWallDataLayer.bids_cumulative[trade.Price-self.BuyWallDataLayer.rolling_window_width]
							for _, x := range self.BuyWallDataLayer.asks_keys { // iterate through asks in increasing order
								if self.BuyWallDataLayer.asks_cumulative[x] >= depth_at_fill_wall*self.sell_safety_factor {
									ideal_price = x
									break
								}
							}
							o := self.createPaperTrade("sell", 0.01, ideal_price, trade.OrderId)
							if o == nil {
								log.Println("WARNING: we failed to create a corresponding sell order for our executed buy order because of an error.")
								goto ExecutingTrades
							}
							o.CorrespondingBuyOrderId = trade.OrderId

							// add new outstanding fill to Fills
							f := Fill{
								CorrespondingBuyOrderId:  trade.OrderId,
								CorrespondingSellOrderId: o.OrderId,
								Size:          trade.Size,
								StopLossPrice: trade.Price - self.BuyWallDataLayer.rolling_window_width,
								Outstanding:   true,
							}
							self.Fills = append(self.Fills, f)

							goto ExecutingTrades

						} else if trade.Side == "sell" {

							// execute
							self.executePaperTrade(trade.OrderId)

							goto ExecutingTrades

						}
					}
				}
			}
		default:
			// if no message, proceed immediately (non-blocking).
		}

	}

	////
	// (2) Check if any of our Stop Losses have been Triggered
	////

ExecutingStopLosses:

	for i, f := range self.Fills {
		if f.Outstanding == true {
			if self.Book.highest_bid <= f.StopLossPrice {
				// Use dynamic limit orders!
				// first find the sell order that corresponds.
				for _, o := range self.openOrders["sell"] {
					if f.CorrespondingSellOrderId == o.OrderId {
						//if o.CorrespondingBuyOrderId == f.CorrespondingBuyOrderId {
						current_offer_price := self.Book.highest_bid + float64(0.01)
						if o.Price != current_offer_price {
							// adjust the price if needed
							o.Price = current_offer_price
							self.cancelPaperTrade(o.OrderId)
							o2 := self.createPaperTrade("sell", o.Size, o.Price, o.CorrespondingBuyOrderId)
							if o2 == nil {
								log.Println("WARNING: we failed to create a new sell order corresponding to our stop loss because of an error.")
								self.Fills[i].Outstanding = false
							} else {
								self.Fills[i].CorrespondingSellOrderId = o2.OrderId
							}
							continue ExecutingStopLosses
						}
						break
					}
				}
				// We do not need to re-raise if we don't get filled at our stoploss price
				// because the price CAN'T go back up without filling our order first.
			}
		}
	}

	//////
	// (3) Ensure that all current LIMIT buy offers are still likely to be profitable.
	// Pull them if they are no longer likely to be profitable.
	//////

	var existing_buy_walls []float64 // we will use this later

	for _, o := range self.openOrders["buy"] {

		bound_upper := o.Price
		bound_lower := o.Price - self.BuyWallDataLayer.rolling_window_width
		good_enough := false

		// Make sure the key has a value > buy_safety_factor
		for k, v := range self.BuyWallDataLayer.experimental {
			if bound_lower < k {
				if k < bound_upper {
					if v > self.buy_safety_factor {
						good_enough = true
					}
				}
			}
		}
		if good_enough {
			existing_buy_walls = append(existing_buy_walls, o.Price)
		} else {
			self.cancelPaperTrade(o.OrderId)
		}
	}

	////
	// (4) Make NEW Buy orders
	////

	for buy_position, _ := range self.BuyWallDataLayer.experimental {
		if self.BuyWallDataLayer.experimental[buy_position] > self.buy_safety_factor {
			if float64InSlice(buy_position, existing_buy_walls) == false {
				if buy_position < self.Book.lowest_ask {
					self.createPaperTrade("buy", 0.01, buy_position, "")
				}
			}
		}
	}

}

func (self *PennyBot) CancelUnprofitableExistingOrders() {
	self.mutex.Lock()
	self.existing_buy_walls = []float64{}
	for _, o := range self.openOrders["buy"] {
		bound_upper := o.Price
		bound_lower := o.Price - self.BuyWallDataLayer.rolling_window_width
		if mapHasKeyInRange(self.BuyWallDataLayer.experimental, bound_lower, bound_upper) == false {
			// Pull the buy order.
			self.cancelPaperTrade(o.OrderId)
		} else {
			// we use this when placingNewBuyOrders
			self.existing_buy_walls = append(self.existing_buy_walls, o.Price)
		}
	}
	self.mutex.Unlock()
}

func (self *PennyBot) RemoveCancelledOrders() {
	for true {
		select {
		case id := <-self.NewlyCancelledOrders:
			self.mutex.Lock()
			log.Println("Processing Newly Cancelled Order")
			for side, trades := range self.openOrders {
				for index, trade := range trades {
					if trade.OrderId == id {
						self.openOrders[side] = removePaperTrade(self.openOrders[side], index)
						//goto CancelOrders
					}
				}
			}
			self.mutex.Unlock()
		default:
			// continue immediately
		}
	}
}

func (self *PennyBot) ManageStopLosses() {
	self.mutex.Lock()
	for i, f := range self.Fills {
		if f.Outstanding == true {
			if self.Book.highest_bid <= f.StopLossPrice {
				// Use dynamic limit orders!
				// first find the sell order that corresponds.
				for _, o := range self.openOrders["sell"] {
					if f.CorrespondingSellOrderId == o.OrderId {
						//if o.CorrespondingBuyOrderId == f.BuyOrderId {
						current_offer_price := self.Book.highest_bid + float64(0.01)
						if o.Price != current_offer_price {
							// adjust the price if needed
							self.cancelPaperTrade(o.OrderId)
							o2 := self.createPaperTrade("sell", o.Size, current_offer_price, o.CorrespondingBuyOrderId)
							if o2 == nil {
								log.Println("WARNING: we failed to create a new sell order corresponding to our stop loss because of an error.")
								self.Fills[i].Outstanding = false
							} else {
								self.Fills[i].CorrespondingSellOrderId = o2.OrderId
							}
							//continue ExecutingStopLosses
							// This can get stuck in a loop when there's "insufficient funds" or whatever error
						}
						break
					}
				}
				// We do not need to re-raise if we don't get filled at our stoploss price
				// because the price CAN'T go back up without filling our order first.
			}
		}
	}
	self.mutex.Unlock()
}

func (self *PennyBot) ManageExecutedOrders() {
	for true {
		select {
		case id := <-self.NewlyExecutedOrders:
			self.mutex.Lock()
			log.Println("Processing Newly Executed Order")
			// We have an updated Order!
			for _, openOrders := range self.openOrders {
				for _, trade := range openOrders {
					if trade.OrderId == id {
						// It was executed but not yet processed/removed!
						if trade.Side == "buy" {

							// execute
							self.executePaperTrade(trade.OrderId)

							// create corresponding sell order
							var ideal_price float64
							// find closest existing key!
							var next_buy_key float64
							bids_keys := self.BuyWallDataLayer.bids_keys
							sort.Float64s(bids_keys)                              // Bids are sorted in increasing order
							sort.Sort(sort.Reverse(sort.Float64Slice(bids_keys))) // Bids are sorted in decreasing order
							for _, x := range bids_keys {
								if x <= trade.Price-self.BuyWallDataLayer.rolling_window_width {
									next_buy_key = x
									break
								}
							}

							depth_at_fill_wall := self.BuyWallDataLayer.bids_cumulative[next_buy_key]
							for _, x := range self.BuyWallDataLayer.asks_keys { // iterate through asks in increasing order
								if self.BuyWallDataLayer.asks_cumulative[x] >= depth_at_fill_wall*self.sell_safety_factor {
									ideal_price = x
									break
								}
							}
							o := self.createPaperTrade("sell", 0.001, ideal_price, trade.OrderId)
							if o == nil {
								log.Println("WARNING: we failed to create a corresponding sell order for our executed buy order because of an error.")
								//goto ExecutingTrades
							}

							o.CorrespondingBuyOrderId = trade.OrderId // < invalid memory address or nil pointer dereference

							// add new outstanding fill to Fills
							f := Fill{
								CorrespondingBuyOrderId:  trade.OrderId,
								CorrespondingSellOrderId: o.OrderId,
								Size:          trade.Size,
								StopLossPrice: trade.Price - self.BuyWallDataLayer.rolling_window_width,
								Outstanding:   true,
							}
							self.Fills = append(self.Fills, f)

							log.Println("CREATED SELL @", ideal_price, "and stoploss @", trade.Price-self.BuyWallDataLayer.rolling_window_width)

							//goto ExecutingTrades

						} else if trade.Side == "sell" {

							// execute
							self.executePaperTrade(trade.OrderId)

							//goto ExecutingTrades

						}
					}
				}
			}
			self.mutex.Unlock()
		default:
			// if no message, proceed immediately (non-blocking).
		}
	}
}

func (self *PennyBot) PlaceNewBuyOrders() {

	self.mutex.Lock()
	self.BuyWallDataLayer.mutex.Lock()

	if self.AvailableBalance["USD"] > 0.001*self.Book.highest_bid {

		balance_left := self.AvailableBalance["USD"]

		var buy_walls []float64
		for _, k := range self.BuyWallDataLayer.buy_walls_keys { // This list is sorted in an order such that values will be in descending order (buy_walls[k] = v)
			// cost = ReducePrecision(k)*0.001
			// position = ReducePrecision(k)
			// v = value

			k = ReducePrecision(k) // This is neccessary or we create duplicate orders!
			// Edit: why the heck is this possible... It should simply be 1 order per 1-cent price point.

			if self.BuyWallDataLayer.buy_walls[k] < self.BuyWallDataLayer.average_buy_size*self.buy_safety_factor {
				break
			} else {
				if balance_left > (k * 0.001) {
					buy_walls = append(buy_walls, k)
					balance_left = balance_left - (k * 0.001)
				} else {
					we_can_continue := false
					// We don't have enough money. Let's see if we can cancel open orders which are FURTHER from the spread AND have less potential.
					for _, o := range self.openOrders["buy"] {
						if self.Book.highest_bid-o.Price > self.Book.highest_bid-k {
							if self.BuyWallDataLayer.experimental[o.Price] < self.BuyWallDataLayer.experimental[k] {
								self.cancelPaperTrade(o.OrderId)
								balance_left = balance_left + (o.Price * 0.001)
								buy_walls = append(buy_walls, k)
								we_can_continue = true
								break
							}
						}
					}
					if we_can_continue == false {
						break
					}
				}
			}
		}

		for _, k := range buy_walls {
			if float64InSlice(k, self.existing_buy_walls) == false {
				bound_upper := k
				bound_lower := k - self.BuyWallDataLayer.rolling_window_width
				if mapHasKeyInRange(self.BuyWallDataLayer.experimental, bound_lower, bound_upper) {
					if k < self.BuyWallDataLayer.Book.lowest_ask-0.03 { // margin of error, due to HFT. Market may have moved.
						self.createPaperTrade("buy", 0.001, k, "")
					}
				}
			}
		}
	}

	self.BuyWallDataLayer.mutex.Unlock()
	self.mutex.Unlock()
}

func (self *PennyBot) update(*sqlx.DB) {

	/*



		Use the rolling window strategy to identify buy walls. Then only utilize them if our CleverUpdate
		formula finds them profitable. Also, pull them if it ever finds them likely to be unprofitable.

		This prevents us from switching to a further-back buy wall which appears, and abandoning the still-profitable
		but smaller buy wall closer to the spread.

		Todo:

			- add mid-market price to Book and start using it for .experimental calculations

			- only make a BUY if the trade will have +EV even after taking sell_safety into account.

			- Add to cleverUpdate algorithm such that while the buy wall is closer than the sell side's equal cumulative depth,
			the point at which you are entering a bid needs to have less cumulative depth than the other side (else you cant get filled!)

	*/

CancelOrders:

	if self.RealMoneyMode == true {
		select {
		case id := <-self.NewlyCancelledOrders:
			log.Println("Processing Newly Cancelled Order")
			for side, trades := range self.openOrders {
				for index, trade := range trades {
					if trade.OrderId == id {
						self.openOrders[side] = removePaperTrade(self.openOrders[side], index)
						goto CancelOrders
					}
				}
			}
		default:
			// continue immediately
		}

	}

	////
	// (1) Check if any of our paper trades should execute
	////

ExecutingTrades:

	if self.RealMoneyMode == false {
		for _, openOrders := range self.openOrders {
			for _, trade := range openOrders {
				if trade.Side == "buy" {
					//if trade.Price >= self.Book.lowest_ask-float64(0.01) {
					if trade.Price >= self.Book.lowest_ask {

						// execute
						self.executePaperTrade(trade.OrderId)

						// create corresponding sell order
						var ideal_price float64
						// find closest existing key!
						var next_buy_key float64
						bids_keys := self.BuyWallDataLayer.bids_keys
						sort.Float64s(bids_keys)                              // Bids are sorted in increasing order
						sort.Sort(sort.Reverse(sort.Float64Slice(bids_keys))) // Bids are sorted in decreasing order
						for _, x := range bids_keys {
							if x <= trade.Price-self.BuyWallDataLayer.rolling_window_width {
								next_buy_key = x
								break
							}
						}

						depth_at_fill_wall := self.BuyWallDataLayer.bids_cumulative[next_buy_key]
						for _, x := range self.BuyWallDataLayer.asks_keys { // iterate through asks in increasing order
							if self.BuyWallDataLayer.asks_cumulative[x] >= depth_at_fill_wall*self.sell_safety_factor {
								ideal_price = x - 0.01 // One step back or this will be at the top of a huge vertical wall.
								break
							}
						}
						o := self.createPaperTrade("sell", 0.001, ideal_price, trade.OrderId)
						if o == nil {
							log.Println("WARNING: we failed to create a corresponding sell order for our executed buy order because of an error.")
							goto ExecutingTrades
						}
						o.CorrespondingBuyOrderId = trade.OrderId

						// add new outstanding fill to Fills
						f := Fill{
							CorrespondingBuyOrderId:  trade.OrderId,
							CorrespondingSellOrderId: o.OrderId,
							Size:          trade.Size,
							StopLossPrice: trade.Price - self.BuyWallDataLayer.rolling_window_width,
							Outstanding:   true,
						}
						self.Fills = append(self.Fills, f)

						goto ExecutingTrades

					}
				} else if trade.Side == "sell" {
					//if trade.Price <= self.Book.highest_bid+float64(0.01) {
					if trade.Price <= self.Book.highest_bid {
						// execute
						self.executePaperTrade(trade.OrderId)

						goto ExecutingTrades
					}
				}

			}
		}
	} else if self.RealMoneyMode == true {
		select {
		case id := <-self.NewlyExecutedOrders:
			log.Println("Processing Newly Executed Order")
			// We have an updated Order!
			for _, openOrders := range self.openOrders {
				for _, trade := range openOrders {
					if trade.OrderId == id {
						// It was executed but not yet processed/removed!
						if trade.Side == "buy" {

							// execute
							self.executePaperTrade(trade.OrderId)

							// create corresponding sell order
							var ideal_price float64
							// find closest existing key!
							var next_buy_key float64
							bids_keys := self.BuyWallDataLayer.bids_keys
							sort.Float64s(bids_keys)                              // Bids are sorted in increasing order
							sort.Sort(sort.Reverse(sort.Float64Slice(bids_keys))) // Bids are sorted in decreasing order
							for _, x := range bids_keys {
								if x <= trade.Price-self.BuyWallDataLayer.rolling_window_width {
									next_buy_key = x
									break
								}
							}

							depth_at_fill_wall := self.BuyWallDataLayer.bids_cumulative[next_buy_key]
							for _, x := range self.BuyWallDataLayer.asks_keys { // iterate through asks in increasing order
								if self.BuyWallDataLayer.asks_cumulative[x] >= depth_at_fill_wall*self.sell_safety_factor {
									ideal_price = x
									break
								}
							}
							o := self.createPaperTrade("sell", 0.001, ideal_price, trade.OrderId)
							if o == nil {
								log.Println("WARNING: we failed to create a corresponding sell order for our executed buy order because of an error.")
								goto ExecutingTrades
							}

							o.CorrespondingBuyOrderId = trade.OrderId // < invalid memory address or nil pointer dereference

							// add new outstanding fill to Fills
							f := Fill{
								CorrespondingBuyOrderId:  trade.OrderId,
								CorrespondingSellOrderId: o.OrderId,
								Size:          trade.Size,
								StopLossPrice: trade.Price - self.BuyWallDataLayer.rolling_window_width,
								Outstanding:   true,
							}
							self.Fills = append(self.Fills, f)

							log.Println("CREATED SELL @", ideal_price, "and stoploss @", trade.Price-self.BuyWallDataLayer.rolling_window_width)

							goto ExecutingTrades

						} else if trade.Side == "sell" {

							// execute
							self.executePaperTrade(trade.OrderId)

							goto ExecutingTrades

						}
					}
				}
			}
		default:
			// if no message, proceed immediately (non-blocking).
		}

	}

	////
	// (2) Check if any of our Stop Losses have been Triggered
	////
	//if self.ETH_wallet_balance > 0 {

ExecutingStopLosses:

	for i, f := range self.Fills {
		if f.Outstanding == true {
			if self.Book.highest_bid <= f.StopLossPrice {
				// Use dynamic limit orders!
				// first find the sell order that corresponds.
				for _, o := range self.openOrders["sell"] {
					if f.CorrespondingSellOrderId == o.OrderId {
						//if o.CorrespondingBuyOrderId == f.BuyOrderId {
						current_offer_price := self.Book.highest_bid + float64(0.01)
						if o.Price != current_offer_price {
							// adjust the price if needed
							self.cancelPaperTrade(o.OrderId)
							o2 := self.createPaperTrade("sell", o.Size, current_offer_price, o.CorrespondingBuyOrderId)
							if o2 == nil {
								log.Println("WARNING: we failed to create a new sell order corresponding to our stop loss because of an error.")
								self.Fills[i].Outstanding = false
							} else {
								self.Fills[i].CorrespondingSellOrderId = o2.OrderId
							}
							continue ExecutingStopLosses
							// This can get stuck in a loop when there's "insufficient funds" or whatever error
						}
						break
					}
				}
				// We do not need to re-raise if we don't get filled at our stoploss price
				// because the price CAN'T go back up without filling our order first.
			}
		}
	}
	//}


	//////
	// (3) Ensure that all current LIMIT buy offers are still likely to be profitable.
	// Pull them if they are no longer likely to be profitable.
	//////

	var existing_buy_walls []float64 // we will use this later

	for _, o := range self.openOrders["buy"] {

		bound_upper := o.Price
		bound_lower := o.Price - self.BuyWallDataLayer.rolling_window_width

		if mapHasKeyInRange(self.BuyWallDataLayer.experimental, bound_lower, bound_upper) == false {
			// Pull the buy order.
			self.cancelPaperTrade(o.OrderId)
		} else {
			existing_buy_walls = append(existing_buy_walls, o.Price)
		}
	}

	//////
	// (4) New Buy Order
	/////

	/*
		tmp := fmt.Sprintf("%.4f", self.BuyWallDataLayer.largest_buy_wall_position+float64(0.01))
		new_buy_order_price, _ := strconv.ParseFloat(tmp, 4)

		// a buy wall exists that is big enough
		if new_buy_order_price < self.Book.lowest_ask {
			if self.BuyWallDataLayer.largest_buy_wall_volume/self.BuyWallDataLayer.average_buy_size > self.buy_safety_factor {
				// and sell_distance > buy_distance {
				//if sell_distance > buy_distance {
				// and it would be a LIMIT order
				// and it would be a new one
				if float64InSlice(new_buy_order_price, existing_buy_walls) == false {
					bound_upper := new_buy_order_price
					bound_lower := new_buy_order_price - self.BuyWallDataLayer.rolling_window_width
					if mapHasKeyInRange(self.BuyWallDataLayer.experimental, bound_lower, bound_upper) {
						//if new_buy_order_price != self.openOrders["buy"][0].Price {
						// replace existing BUY order if new one is different
						//self.cancelPaperTrade(self.openOrders["buy"][0].OrderId)
						self.createPaperTrade("buy", 0.01, new_buy_order_price, "")
					}
				}
			}
		}
	*/

	if self.AvailableBalance["USD"] > 0.001*self.Book.highest_bid {

		balance_left := self.AvailableBalance["USD"]

		var buy_walls []float64
		for _, k := range self.BuyWallDataLayer.buy_walls_keys { // This list is sorted in an order such that values will be in descending order (buy_walls[k] = v)
			// cost = ReducePrecision(k)*0.001
			// position = ReducePrecision(k)
			// v = value

			k = ReducePrecision(k) // This is neccessary or we create duplicate orders!

			if self.BuyWallDataLayer.buy_walls[k] < self.BuyWallDataLayer.average_buy_size*self.buy_safety_factor {
				break
			} else {
				if balance_left > (k * 0.001) {
					buy_walls = append(buy_walls, k)
					balance_left = balance_left - (k * 0.001)
				} else {
					we_can_continue := false
					// We don't have enough money. Let's see if we can cancel open orders which are FURTHER from the spread AND have less potential.
					for _, o := range self.openOrders["buy"] {
						if self.Book.highest_bid-o.Price > self.Book.highest_bid-k {
							if self.BuyWallDataLayer.experimental[o.Price] < self.BuyWallDataLayer.experimental[k] {
								self.cancelPaperTrade(o.OrderId)
								balance_left = balance_left + (o.Price * 0.001)
								buy_walls = append(buy_walls, k)
								we_can_continue = true
								break
							}
						}
					}
					if we_can_continue == false {
						break
					}
				}
			}
		}

		for _, k := range buy_walls {
			if float64InSlice(k, existing_buy_walls) == false {
				bound_upper := k
				bound_lower := k - self.BuyWallDataLayer.rolling_window_width
				if mapHasKeyInRange(self.BuyWallDataLayer.experimental, bound_lower, bound_upper) {
					if k < self.BuyWallDataLayer.Book.lowest_ask-0.03 { // margin of error, due to HFT. Market may have moved.
						self.createPaperTrade("buy", 0.001, k, "")
					}
				}
			}
		}
	}

}

func (self *PennyBot) run() {
	// The below run forever.
	go self.UpdateGDAXOrders()
	go self.UpdateBalances()
	go self.ManageExecutedOrders()
	go self.RemoveCancelledOrders()
	go self.cacheStream()
	// The below run and return.
	for _ = range self.channel {
		go self.PlaceNewBuyOrders()
		go self.ManageStopLosses()
		go self.CancelUnprofitableExistingOrders()
	}
}

func (b *PennyBot) cacheStream() {
	for true {
		time.Sleep(100)
		b.mutex.Lock()
		b.BuyWallDataLayer.mutex.Lock()

		bids_keys := b.BuyWallDataLayer.bids_keys
		asks_keys := b.BuyWallDataLayer.asks_keys

		bids_cumulative := b.BuyWallDataLayer.bids_cumulative
		asks_cumulative := b.BuyWallDataLayer.asks_cumulative

		sort.Float64s(bids_keys) // Bids are sorted in increasing order
		sort.Float64s(asks_keys) // Asks are sorted in increasing order

		s := `{"bids":[`
		for k := range bids_keys {
			s += "[" + fmt.Sprintf("%.4f", bids_keys[k]) + "," + fmt.Sprintf("%.4f", bids_cumulative[bids_keys[k]]) + "],"
		}
		s = s[0 : len(s)-1]
		s += `],"asks":[`
		for k := range asks_keys {
			s += "[" + fmt.Sprintf("%.4f", asks_keys[k]) + "," + fmt.Sprintf("%.4f", asks_cumulative[asks_keys[k]]) + "],"
		}
		s = s[0 : len(s)-1]
		s += `]`
		if len(b.openOrders["buy"]) > 0 {
			s += `,"buy_orders":[`
			for _, o := range b.openOrders["buy"] {
				s += fmt.Sprintf("%.4f", o.Price) + ","
			}
			s = s[0 : len(s)-1]
			s += `]`
		} else {
			s += `,"buy_orders":[]`
		}
		if len(b.openOrders["sell"]) > 0 {
			s += `,"sell_orders":[`
			for _, o := range b.openOrders["sell"] {
				s += fmt.Sprintf("%.4f", o.Price) + ","
			}
			s = s[0 : len(s)-1]
			s += `]`
		} else {
			s += `,"sell_orders":[]`
		}

		s += `,"stoplosses":[`
		x := 0
		for _, f := range b.Fills {
			if f.Outstanding == true {
				s += fmt.Sprintf("%.4f", f.StopLossPrice) + ","
				x += 1
			}
		}
		if x > 0 {
			s = s[0 : len(s)-1]
		}
		s += `]`

		s += `,"wallet_balance":` + fmt.Sprintf("%.2f", float64(b.ETH_wallet_balance)/100)

		s += "}"
		b.cached_stream = s

		b.mutex.Unlock()
		b.BuyWallDataLayer.mutex.Unlock()

	}
}

func (b *PennyBot) streamActivity(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "%v", b.cached_stream)
}

type Page struct {
	Title string
	Body  []byte
}

func depthChart(w http.ResponseWriter, r *http.Request) {
	t, _ := template.ParseFiles("templates/depth.html")
	t.Execute(w, Page{Title: "Depth Chart"})
}

func (self *OrderBookEngine) StartClient() {
	self.Client = gdax.NewClient(self.Configuration.GDAX["Secret"], self.Configuration.GDAX["Key"], self.Configuration.GDAX["Pass"])
	tr := &http.Transport{
		MaxIdleConns:        10,
		MaxIdleConnsPerHost: 10,
	}
	self.Client.HttpClient = &http.Client{Timeout: 75 * time.Second, Transport: tr}

	var wsDialer ws.Dialer
	var err error = nil
	self.wsConn, _, err = wsDialer.Dial("wss://ws-feed.gdax.com", nil)
	if err != nil {
		println(err.Error())
	}

	subscribe := map[string]string{
		"type":       "subscribe",
		"product_id": self.Pair,
	}
	if err := self.wsConn.WriteJSON(subscribe); err != nil {
		println(err.Error())
	}
	fmt.Println("subscribed!")
	time.Sleep(4)

	self.updates = make(chan *gdax.Message)

}

func (self *OrderBookEngine) KillClient() {
	// If we are here, then we hit a websocket problem and need to restart the websocket connection.
	self.Kill_All_Existing_Goroutines <- true
	//close(Kill_All_Existing_Goroutines) // kill them. a close() signal is recieved by all running goroutines
}

func main() {

	f, err := os.OpenFile("log.txt", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		panic("error opening log file")
	}
	defer f.Close()
	log.SetOutput(f)

	file, _ := os.Open("config.json")
	decoder := json.NewDecoder(file)
	configuration := Configuration{}
	err = decoder.Decode(&configuration)
	if err != nil {
		log.Fatalln(err)
	}

	db, err := sqlx.Connect("postgres", "user="+configuration.Database["User"]+" password="+configuration.Database["Pass"]+" dbname="+configuration.Database["Name"]+" sslmode=disable")
	if err != nil {
		log.Fatalln(err)
	}

	pennyBot1 := PennyBot{
		sell_safety_factor: 0.7,
		buy_safety_factor:  4, // this works diff for update() .. 10-30 seems reasonable
	}
	pennyBot1.Id = 1
	pennyBot1.Name = "PennyBot1_Normal"
	pennyBot1.RealMoneyMode = false
	pennyBot1.db = db
	pennyBot1.init()

	http.HandleFunc("/ajax/depth", pennyBot1.streamActivity)
	http.HandleFunc("/", depthChart)
	go http.ListenAndServe(":9999", nil)

	//

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		for sig := range c {
			// sig is a ^C, handle it
			fmt.Println(sig)
			pennyBot1.cancelAllGDAXOrders()
			//pennyBot2.cancelAllGDAXOrders()
			panic(sig)
			// os.Exit()
		}
	}()

	// Start book
	book := OrderBookEngine{}
	book.Configuration = &configuration
	book.Pair = "ETH-USD"
	book.Kill_All_Existing_Goroutines = make(chan bool)

	buyWallDataLayer := NewBuyWallDataLayer(&book)
	book.DataLayers = append(book.DataLayers, &buyWallDataLayer)
	buyWallDataLayer.recievers = append(buyWallDataLayer.recievers, &pennyBot1)

	go book.runBITMEX()
	//go buyWallDataLayer.run()

	pennyBot1.Book = &book
	pennyBot1.BuyWallDataLayer = &buyWallDataLayer

	// Async methods/loops

	//go pennyBot1.run()

	//

	for true {

		fmt.Printf("%v: bid ask spread %.2f %.2f %.2f (%v Bids, %v Asks in Book, %v bids & %v asks in BWDL) wall %.2f ETH @ %.2f depth=%.2f, avg= %.2f\n",
			time.Now().Format("2006-01-02 15:04:05"),
			book.highest_bid,
			book.lowest_ask,
			book.spread,
			len(book.Book.Bids),
			len(book.Book.Asks),
			len(buyWallDataLayer.bids),
			len(buyWallDataLayer.asks),
			buyWallDataLayer.largest_buy_wall_volume,
			buyWallDataLayer.largest_buy_wall_position,
			buyWallDataLayer.largest_buy_wall_depth,
			buyWallDataLayer.average_buy_size,
		)

		tmp := spew.Sdump(pennyBot1.AvailableBalance)
		tmp = tmp + ""

		time.Sleep(1000)

	}

}
