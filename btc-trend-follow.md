//@version=4
// Created by Half
strategy("Crypto Killer w/ TF", overlay=true, pyramiding=1, default_qty_type= strategy.percent_of_equity, default_qty_value = 90, calc_on_order_fills=false, slippage=0,commission_type=strategy.commission.percent,commission_value=0.075)
strat_dir_input = input(title="Strategy Direction", defval="all", options=["long", "short", "all"])
strat_dir_value = strategy.direction.all
//////////////////////////////////////////////////////////////////////
// Testing Start dates
testStartYear = input(2013, "Backtest Start Year")
testStartMonth = input(1, "Backtest Start Month")
testStartDay = input(1, "Backtest Start Day")
testPeriodStart = timestamp(testStartYear,testStartMonth,testStartDay,0,0)
//Stop date if you want to use a specific range of dates
testStopYear = input(2030, "Backtest Stop Year")
testStopMonth = input(12, "Backtest Stop Month")
testStopDay = input(30, "Backtest Stop Day")
testPeriodStop = timestamp(testStopYear,testStopMonth,testStopDay,0,0)


testPeriod() =>
    time >= testPeriodStart and time <= testPeriodStop ? true : false
// Component Code Stop
//////////////////////////////////////////////////////////////////////
//INPUT
src = input(close, title="Source")
modeSwitch = input("Hma", title="Hull Variation", options=["Hma", "Thma", "Ehma"])
length = input(48, title="Length(48 for swing, 200 for LT entry)")
length1 = input (1000, title="HMA Trend Filter Length")
switchColor = input(true, "Color Hull according to trend?")
candleCol = input(false,title="Color candles based on Hull's Trend?")
visualSwitch  = input(true, title="Show as a Band?")
thicknesSwitch = input(1, title="Line Thickness")
transpSwitch = input(40, title="Band Transparency",step=5)

//FUNCTIONS
//HMA
HMA(_src, _length) =>  wma(2 * wma(_src, _length / 2) - wma(_src, _length), round(sqrt(_length)))
//EHMA    
EHMA(_src, _length) =>  ema(2 * ema(_src, _length / 2) - ema(_src, _length), round(sqrt(_length)))
//THMA    
THMA(_src, _length) =>  wma(wma(_src,_length / 3) * 3 - wma(_src, _length / 2) - wma(_src, _length), _length)
    
//SWITCH
Mode(modeSwitch, src, len) =>
      modeSwitch == "Hma"  ? HMA(src, len) :
      modeSwitch == "Ehma" ? EHMA(src, len) : 
      modeSwitch == "Thma" ? THMA(src, len/2) : na
      
//OUT
HULL = Mode(modeSwitch, src, length)
MHULL = HULL[0]
SHULL = HULL[2]

TREND = Mode(modeSwitch, src, length1)
TFF = TREND[0]
TFS = TREND[1]

//COLOR
hullColor = switchColor ? (HULL > HULL[2] ? #00ff00 : #ff0000) : #ff9800
tfcolor = switchColor ? (TREND > TREND[1] ? #00ff00 : #ff0000) : #ff9800

//PLOT
///< Frame
Fi1 = plot(MHULL, title="MHULL", color=hullColor, linewidth=thicknesSwitch, transp=50)
Fi2 = plot(visualSwitch ? SHULL : na, title="SHULL", color=hullColor, linewidth=thicknesSwitch, transp=50)
Fi3 = plot(TFF, title="TF", color=hullColor, transp=50)

///< Ending Filler
fill(Fi1, Fi2, title="Band Filler", color=hullColor, transp=transpSwitch)
///BARCOLOR
barcolor(color = candleCol ? (switchColor ? hullColor : na) : na)


if TREND[0] > TREND [1] and HULL[0] > HULL[2] and testPeriod()
    strategy.entry("buy", long=true)
if TREND[0] > TREND [1] and HULL[0] < HULL[2] and testPeriod()
    strategy.close_all()
if TREND[0] < TREND [1] and HULL[0] < HULL[2] and testPeriod()
    strategy.entry("sell", long=false)
if TREND[0] < TREND [1] and HULL[0] > HULL[2] and testPeriod()
    strategy.close_all()