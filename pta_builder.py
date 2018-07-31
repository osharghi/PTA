from datetime import datetime
from sys import argv
import io
import time
import argparse

from icemap.channel_reader import read_frame
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

parser = argparse.ArgumentParser(description='Categorize arguments for channel_reader')
parser.add_argument('month', type=int, help='Month of capture')
parser.add_argument('start_day', type=int, help='Start day of capture')
parser.add_argument('end_day', type=int, help='End day of capture')
parser.add_argument('year', type=int, help='Year of capture')
parser.add_argument('path1', type=str, help='Path of matches capture')
parser.add_argument('path2', type=str, help='Path of orders capture')
args = parser.parse_args()


def read_file(month, start_day, end_day, year, path1, path2):

    """Uses channel_reader.py to obtain dataframes of the matches and orders
        protobuf files. Dataframes are used to create a Post Trade Analysis pdf.

        ``month``, ``start_day``, ``end_day``, and ``year`` are used to build a
        path to the protobuf files. ``path1`` is the path to the matches protobuf
        and ``path2`` is the path to the orders protobuf.

        :param month: month of captures
        :param start_day: starting day of captures
        :param end_day: ending day of captures
        :param year: year of captures
        :param path1: path to matches protobuf
        :param path2: path to orders protobuf

        """

    matches_df = read_frame('matches', datetime(year, month, start_day), datetime(year, month, end_day), path1)
    extract_recv_time(matches_df)
    matches_df['price'] = matches_df['price'] / 1000000

    orders_df = read_frame('orders', datetime(year, month, start_day), datetime(year, month, end_day), path2)
    extract_timestamp(orders_df)
    orders_df['price'] = orders_df['price'] / 1000000
    orders_df['qty'] = orders_df['qty'] / 100000000

    combined_df = pd.merge_asof(orders_df, matches_df, on='recv_time', direction='backward')
    pta_dict = get_pta(combined_df)

    fig, (ax1, ax2, ax3) = plt.subplots(nrows=3, ncols=1, figsize=(15,15))

    plt.subplots_adjust(hspace=0.9)

    gross_pnl = "{:,.2f}".format(pta_dict['gross_pnl'])
    net_pnl = "{:,.2f}".format(pta_dict['net_pnl'])
    fees = "{:,.2f}".format(pta_dict['fees'])
    contracts = "{:,}".format(pta_dict['contracts'])
    btc_holdings = "{:,}".format(pta_dict['btc_holdings'])

    s = ("Total Gross PNL: $%s\n\n"
         "Total Net PNL: $%s\n\n"
         "Total Fees: $%s \n\n"
         "Contracts Traded: %s\n\n"
         "BTC Holdings: %s\n" % (gross_pnl, net_pnl, fees, contracts, btc_holdings))

    fig.text(0.1, 0, s, fontsize=13)
    fig.suptitle('Post Trade Analysis', ha='center', fontsize=14)

    matches_series = pd.Series(matches_df['price'].values, index=pd.to_datetime(matches_df['recv_time'], unit='ns').values)
    matches_series.plot(legend=False, ax=ax1)
    ax1.set_xlabel('recv_time')
    ax1.set_ylabel('price')
    ax1.set_title("BTC Price")

    gross_series = pd.Series(combined_df['gross_pnl'].values,
                             index=pd.to_datetime(combined_df['recv_time'], unit='ns').values)
    gross_series.plot(legend=False, ax=ax2)

    net_series = pd.Series(combined_df['net_pnl'].values,
                           index=pd.to_datetime(combined_df['recv_time'], unit='ns').values)
    net_series.plot(legend=False, ax=ax2)

    ax2.set_xlabel('recv_time')
    ax2.set_ylabel('price')
    ax2.set_title("Gross and Net P&L")

    ax3.step(combined_df['recv_time'], combined_df['holdings'])
    ax3.set_xlabel('recv_time')
    ax3.set_ylabel('Holdings')
    ax3.set_title("Position")

    fig = plt.gcf()
    plt.show()
    fig.savefig('pta.pdf')

def get_pta(combined_df):

    fee = .003
    cash = 0
    gross_pnl = 0
    fees = 0
    contracts = 0
    btc_holdings = 0
    total_fees = 0

    holdings_history = []
    fee_history = []
    gross_pnl_history =[]
    net_pnl_history = []

    for index, row in combined_df.iterrows():

        transaction_fee = 0

        if row['side_x'] == 1:
            cash -= row['qty_x'] * row['price_x']
            gross_pnl = cash + row['qty_x'] * row['price_y']
            btc_holdings += row['qty_x']

        else:
            cash += row['qty_x'] * row['price_x']
            gross_pnl = cash + row['qty_x'] * row['price_y']
            btc_holdings -= row['qty_x']

        if row['aggresor_indicator'] == 'True':
            transaction_fee = row['qty_x'] * row['price_x'] * fee
            total_fees += transaction_fee

        holdings_history.append(btc_holdings)
        contracts += row['qty_x']
        fee_history.append(transaction_fee)
        gross_pnl_history.append(gross_pnl)
        net_pnl_history.append(gross_pnl - transaction_fee)

    add_columns(combined_df, holdings_history, fee_history, gross_pnl_history, net_pnl_history)
    net_pnl = gross_pnl - fees
    pta_dict = {'gross_pnl': gross_pnl, 'net_pnl': net_pnl, 'fees': total_fees, 'contracts': contracts,
                'btc_holdings': btc_holdings}

    return pta_dict

def add_columns(combined_df, holdings_history, fee_history, gross_pnl_history, net_pnl_history):

    sLength = len(combined_df['qty_x'])

    combined_df['holdings'] = np.random.randn(sLength)
    combined_df['holdings'] = np.array(holdings_history)

    combined_df['fee'] = np.random.randn(sLength)
    combined_df['fee'] = np.array(fee_history)

    combined_df['gross_pnl'] = np.random.randn(sLength)
    combined_df['gross_pnl'] = np.array(gross_pnl_history)

    combined_df['net_pnl'] = np.random.randn(sLength)
    combined_df['net_pnl'] = np.array(net_pnl_history)


def extract_recv_time(df):

    matches_metadata = df['metadata']

    sLength = len(df['qty'])
    df['recv_time'] = np.random.randn(sLength)

    time_list = []

    for item in matches_metadata:
        time_list.append(getattr(item, 'recv_time'))

    df['recv_time'] = np.array(time_list)

    return df

def extract_timestamp(df):

    matches_metadata = df['metadata']

    sLength = len(df['qty'])
    df['recv_time'] = np.random.randn(sLength)

    time_list = []

    for item in matches_metadata:
        time_list.append(getattr(item, 'timestamp'))

    df['recv_time'] = np.array(time_list)

    return df

if __name__ == "__main__":
    read_file(args.month, args.start_day, args.end_day, args.year, args.path1, args.path2)

