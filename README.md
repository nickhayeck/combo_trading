# Trading Combo
A combo is an options structure that is constructed by either purchasing a call option and selling a put option or buying a call and selling a put. This has the effect of creating a "synthetic long" position which replicates the payout of the underlying security. One can calculate the bounds in which these contracts must trade based on the price of the underlying instrument. 

Written in Rust, this library seeks to trade this arbitrage in cryptocurrency markets. In its heyday (rip SBF lol), it traded options on FTX US Derivatives and their underlying cryptocurrencies on Binance.

Developed in collaboration with [@haydngwyn](https://github.com/haydngwyn)
