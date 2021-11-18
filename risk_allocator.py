import pandas as pd
import numpy as np
import datetime as dt
import os


def read_generic_file(fpath, fname) -> pd.DataFrame:
    '''
    reads pricing data file assuming a zero-indexed date column - can generalize to specify later
    :param fpath: filepath as raw string
    :param fname: filename as raw string of "name.ftype" structure
    :return: pandas dataframe with time-series pricing data, should automatically pick up what type of file to read
    '''

    full_path = os.path.join(fpath, fname)

    #name not important, init as "_"
    _, ftype = fname.split('.')
    if ftype == 'xlsx':
        raw_df = pd.read_excel(full_path, index_col=0)

    elif ftype == 'csv':
        raw_df = pd.read_csv(full_path, index_col=0)

    else:
        #try-except block could work but tbh im too lazy and im writing this on vacation
        print(f'File type "{ftype}" is not a valid filetype. Please try again.')
        return -1

    #bad habit of dropping rows with null... could ffill or bfill here, i choose not to though. sklearn could help handle
    df = raw_df.dropna()
    return df

def generate_breakout(df, fma = 20, sma = 100) -> pd.DataFrame:
    '''
    geberate breakout distances based on fast and slow moving averages. simple strategy with greater distance == greater signal (more conviction)
    the purpose of this is to generate actionable signals to build the risk-engine algorithm from - not academically researched or proven, as good as random
    :param df: pandas frame containing returns data from which to generate signals
    :param fma: fast moving average window size
    :param sma: slow moving average window size
    :return: dataframe containing time-series breakout distances for each ticker
    '''
    return_df = df.copy()
    #iterates tickers
    for col in df:
        #initializing price series instead of grab-and-go from df makes this *marginally* faster
        series_calc = df[col]

        #generate each ticker's fma and sma
        fma_calc = series_calc.rolling(fma).mean()
        sma_calc = series_calc.rolling(sma).mean()

        #if dist negative, momentum down and breakout down - could start to do long/short here, will look into
        dist = fma_calc - sma_calc
        return_df[col] = dist

    #again... lazy implementation that could be back or forward filled :-)
    return return_df.dropna()

def generate_returns(df) -> pd.DataFrame:
    '''
    generates generic returns of an asset over period of time using logarithmic method
    :param df: dataframe of price data associated with each ticker
    :return: dataframe containing non-strategy returns of each ticker
    '''

    #iterate tickers
    for col in df:
        #self explanatory man, idk
        df[col] = np.log(df[col] / df[col].shift(1))

    #first row will always be NaN due to shift - drop it!
    return df.dropna()



def optimal_portfolio(df, max_start = 0.05, max_ongoing = 0.10, max_positions = 30, risk_allocation = 1.00,
                      portfolio = dict(), long_short = False) -> pd.DataFrame:
    '''
    basically what i am trying to do is
    :param df:
    :param max_start:
    :param max_ongoing:
    :param max_positions:
    :param risk_allocation:
    :param portfolio:
    :param long_short:
    :return:
    '''

    for idx, (date, row) in enumerate(df.iterrows()):
        sorted_vals = row.sort_values()
        diction = sorted_vals.iloc[-max_positions : ].to_dict()
        full_signals = row.to_dict()
        #TODO: this needs to be changed to reflect L/S
        change_dict = {key : (val, 1) for key, val in diction.items()}
        port, exposure = calculate_portfolio(max_start, max_ongoing, max_positions, risk_allocation, portfolio, change_dict,
                                   full_signals, long_short)


        if idx == 0:
            iteration = f'{idx+1}st Iteration: \n'

        elif idx == 1:
            iteration = f'{idx+1}nd Iteration: \n'

        elif idx == 2:
            iteration = f'{idx+1}rd Iteration: \n'

        else:
            iteration = f'{idx+1}th Iteration: \n'

        print(iteration)
        print(f'Portfolio: {port}')
        print(f'Net Exposure: {exposure}')
    return port
        
def sort_dict(diction, key_val = 'key', high_to_low = True) -> dict:
    pos = 0 if key_val == 'key' else 1
    return {k: v for k, v in sorted(diction.items(), key=lambda item: item[pos], reverse=high_to_low)} 
    

def calculate_portfolio(max_start = 0.05, max_ongoing = 0.10, max_positions = 30, risk_allocation = 1.00,
                  portfolio = dict(), change_dict = dict(), total_signals = dict(), long_short = False) -> dict:


    #TODO: RESCALE AFTER EACH ITERATION - dropping a lot of net exposure for no good reason, must be done after name rebal
    '''
    size positions according to blatantly arbitrary parameters
    :param max_start: max starting position - a decimal point representation of percent allocation
    :param max_ongoing: max ongoing position - a decimal point representation of percent allocation
    :param max_positions: max number of starting positions - must be an integer value
    :param risk_allocation: max net exposure - a decimal point representation of percent exposure
    :param portfolio: dictionary containing current portfolio values - form {ticker : port. allocation)
    :param change_dict: dictionary containing proposed portfolio changes - form {ticker : (port. allocation, long/short))
    :param long_short: boolean representing whether strategy can go net short - True/False
    :return: dictionary containing new positions and allocations - form {ticker : port. allocation)
    '''

    #making sure things all match up
    assert type(max_positions) == int
    assert type(max_start) == float
    assert type(max_ongoing) == float
    assert type(risk_allocation) == float
    assert type(portfolio) == dict
    assert type(change_dict) == dict
    assert type(long_short) == bool

    #TODO: refactor to take L/S into account here

    #portfolio and change_dict are dicts of structure {ticker (AAPL) : position size (0.0345)}

    #look at each proposed position and decide whether it will be added to portfolio
    port_set = set(portfolio)

    #higher signal == greater value to portfolio

    #in breakout case, larger return distance from 0 ~indicates greater momentum. could add secondary momentum
    #indicators to understand convexity, concavity but these will be implemented as part of a signal generation functions


    scaling_base = np.array([val[0] for val in change_dict.values()]).sum()

    #this indicates full
    scaled_allocation = {key : (val[0] / scaling_base) for key, val in change_dict.items()}
    scaled_set = set(scaled_allocation)
    #find intersection of sets
    retained_positions = port_set & scaled_set

    #NOTE: these are the signal distances, do not need to be constrained
    for position, size in scaled_allocation.items():
        num_positions = len(portfolio)



        #ongoing position
        if position in retained_positions:
            portfolio[position] = size if size < max_ongoing else max_ongoing

        #new position
        else:
            portfolio[position] = size if size < max_start else max_start

        # finds allocations exposures and adds them. this should be checked before inserting, alas i am lazy
        if num_positions >= max_positions:
            position_ordered = sort_dict(portfolio, key_val='val', high_to_low=True)

            #this selection statement makes me want to vomit
            current_worst = list(position_ordered.items())[:max_positions][0][0]

            if portfolio[current_worst] < size:
                portfolio.pop(current_worst)

            else:
                portfolio.pop(position)

        #generate net exposure to scale into risk budget
        exposure = np.array(list(portfolio.values())).sum()

        #simple scaling function to return net exposure to desired levels. may be able to apply leverage here?
        portfolio = {key : (risk_allocation * size) / exposure for key, size in portfolio.items()}

    #return allocation-sorted dictionary and net exposure
    return sort_dict(portfolio, key_val='val', high_to_low=True), exposure


def current_portfolio_composition(price_df : pd.DataFrame, ticker_dict : dict, capital = 1_000_000) -> dict:
    shares_held = dict()
    leftover_capital = 0.00
    for ticker, allocation in ticker_dict.items():
        curr_price = price_df[ticker].iloc[-1]
        raw_allocation = capital * allocation

        #whole number of shares
        shares_held[ticker] = raw_allocation // curr_price

        #have not figured out how i want to spend spare capital yet... roll into next best name?
        leftover_capital += raw_allocation % curr_price

    return shares_held, leftover_capital
        

def main():

    # fpath = r'/Users/christianliddiard/Programming/Python/PFAT'
    fpath = os.path.dirname(__file__)
    fname = r'all_sp_data.csv'
    df = read_generic_file(fpath, fname)

    returns_df = generate_returns(df)

    breakout_df = generate_breakout(returns_df)

    port = optimal_portfolio(breakout_df, max_positions=30, risk_allocation=1.5)
    price_df = read_generic_file(fpath, fname)
    shares_held, leftover_capital = current_portfolio_composition(price_df, port, capital=1000000)
    print(shares_held)
    print(f'${leftover_capital:,.2f} of capital leftover')
    # test_initialization()
    



if __name__ == '__main__':
    main()